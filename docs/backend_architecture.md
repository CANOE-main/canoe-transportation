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

The tree below maps the implemented entrypoints and their owners. An upstream-specific
`canoe_adapter.py` is still a planned integration seam, not an implemented entrypoint. #to-review

```text
.
├── AGENTS.md                                # Stable repository policy
├── README.md                                # Human-facing project orientation
├── .gitattributes                           # Portable text/binary checkout and registered source-byte exceptions #to-review
├── .agents/
│   ├── PLANS.md                             # ExecPlan protocol
│   ├── plans/                               # Task-local implementation records
│   └── skills/                              # Optional task-retrieved procedures
├── config/
│   ├── paths.yaml                           # Canonical paths and artifact-family impact routes
│   ├── sources.yaml                         # External-source registry, provenance and explicit update policies #to-review
│   ├── scenarios/                           # Scenario authoring contract
│   └── parameters/
│       ├── rules.yaml                       # Extraction and harmonization contracts
│       └── conversion.yaml                  # Reusable conversion factors
├── src/
│   ├── setup.py                             # Configuration/schema smoke entrypoint
│   ├── build_transport.py                   # Transport contribution and atomic database assembly
│   ├── fetching/                            # Upstream download, cache, and interim normalization
│   │   ├── source_updater.py                # Explicit rolling-source review/adoption, immutable snapshots and diff logs #to-review
│   │   ├── nrcan_ceud.py                    # NRCan CEUD tables and checksum-pinned fuel-consumption ratings #to-review
│   │   ├── vehicle_population.py            # Ontario MTO report acquisition and normalization
│   │   ├── statcan_tables.py                # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py                # CER energy future tables
│   │   ├── nlr_atb_autonomie.py             # NLR ATB and ANL Autonomie inputs
│   │   ├── greet_automation.py              # Explicit disposable-copy Excel generation of vehicle-cycle evidence
│   │   ├── greet_vehicle_cycle.py           # Registered source contracts, extraction and offline bank validation
│   │   ├── fueleconomy_vehicles.py          # Shared pinned vehicle-class and diagnostic range evidence #to-review
│   │   ├── legacy_charging_profiles.py      # Immutable NHTS/TTS charging evidence and legacy hourly transformation
│   │   ├── epa_omega_baseline.py            # Compact legacy OMEGA baseline sales/CD ranges and source identity validation
│   │   ├── technology_growth_evidence.py    # Registered adoption evidence acquisition and normalization
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
│   │   ├── road_fleet_weights.py            # Shared production class/powertrain fleet composition #to-review
│   │   ├── vehicle_mapping_bootstrap.py     # Explicit mapping-development entrypoint
│   │   ├── road_efficiencies.py             # Road technology efficiencies
│   │   ├── offroad_efficiencies.py          # Off-road technology efficiencies
│   │   ├── road_capex_opex.py               # Road investment and operating costs
│   │   ├── offroad_capex_opex.py            # Off-road investment and operating costs
│   │   ├── currency.py                      # CER-backed currency and price-year conversion
│   │   ├── road_embodied_emissions.py       # Road vehicle-cycle lifetime gases and regional aggregation
│   │   ├── ev_chargers.py                   # EV charging infrastructure preparation and validated artifacts
│   │   ├── ldv_charging_profiles.py         # Inherited hourly profiles, configured scaling and annual utilization limits #to-review
│   │   ├── ldv_ev_ranges.py                 # OMEGA range shares, minimum constraints and parameter-specific representative LDV aggregation
│   │   └── road_adoption_constraints.py     # Opt-in adoption evidence diagnostics and provisional calibration #to-review
│   ├── utils/
│   │   ├── __init__.py                      # Typed config loading and artifact path resolution
│   │   ├── files.py                         # Shared hashing and atomic CSV/text publication #to-review
│   │   ├── build_logging.py                 # Persistent scenario/YAML/UTC-named build logs #to-review
│   │   └── vehicle_labels.py                # Shared vehicle-label mechanics
│   └── validation/
│       ├── config_models.py                 # Pydantic configuration contracts
│       ├── config_smoke.py                  # Setup-time config/schema status and directory creation
│       ├── provenance.py                    # Source and dataset provenance
│       ├── schema_contract.py               # canoe-schema v4 compatibility
│       ├── insertion.py                     # Scoped pre-insertion cleanup and validated insertion
│       ├── database_bootstrap.py            # Post-insertion database integrity checks
│       ├── sqlite_utils.py                  # Shared SQLite identifier mechanics
│       ├── legacy_compare.py                # Narrow configured legacy comparison
│       ├── sqlite_compare.py                # Scenario-to-scenario SQLite comparison #to-review
│       ├── notebook_evidence.py             # Captured, hash-validated stock/efficiency evidence bundles #to-review
│       ├── vehicle_stock_diagnostic.py      # Reader and views for recorded stock/mapping/retention evidence #to-review
│       ├── ldv_efficiency_diagnostic.py     # Reader and views for recorded efficiency evidence #to-review
│       ├── rating_stock_benchmark.py        # Diagnostic MTO/rating comparison, outside production weights #to-review
│       ├── road_investment_review.py        # Opt-in investment-cost interpretation and review #to-review
│       └── transport_comparison.py         # Configured legacy parameter-comparison evidence/report #to-review
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
│   ├── validation/                          # Build reports, source-update audits and recorded notebook bundles #to-review
│   └── logs/                                # Persistent build attempts and source-update logs #to-review
├── docs/
│   ├── backend_architecture.md              # Repository structure and ownership reference
│   ├── canoe_main_orchestrator.md           # Point-in-time upstream integration context; reverify before use #to-review
│   ├── etl_flowcharts.md                    # Parameter-specific lineage reference
│   ├── assumptions.md                      # Enduring data challenges and modelling assumptions #to-review
│   ├── source_inventory.md                 # Registry-based source uses, formats and footprint snapshot #to-review
│   ├── executive_summary.md                # Narrative project/methodology overview #to-review
│   ├── codebase_diagnostic_snapshot.md      # Point-in-time structure/refactor assessment #to-review
│   └── insights/                            # Four marimo diagnosis notebooks; see diagnostic workflows below #to-review
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

The pinned package owns both v4 DDL and final row models. Database creation uses
`canoe_schema.get_sql_schema("4.0")`; insertion registers provenance, validates the
corresponding row model, rejects conflicting keys, then checks foreign keys and integrity
before publication. Local legacy SQL is comparison evidence. #to-review

## Running and extending the backend

The supported scenario command is `uv run python src/build_transport.py --scenario
config/scenarios/default.yaml`; add `--overwrite` to replace its configured database.
It owns the complete production lifecycle through `prepare_scenario_inputs`, followed by
shared contribution preparation, validated insertion and atomic SQLite publication.
Scenario selections stay in YAML. `setup.py` and `scripts/doctor.py` are optional
configuration/schema and readiness checks, not mandatory build stages. See the
[README](../README.md) for installation and the concise operating commands. #to-review

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

Every scenario attempt records a persistent log through `utils.build_logging`. The
`outputs.build_log` template in `paths.yaml` includes the scenario name, scenario YAML
name and UTC timestamp; the log records the YAML path/hash, stage timings and failure
details. Ordinary logs use INFO, with detailed audits enabled by `--debug`. #to-review

## Explicit source updates

`fetching.source_updater` is a maintenance entrypoint outside the scenario lifecycle.
The typed `updater` limits and per-source `update` policies in `sources.yaml` determine
eligible public endpoints and allowed download hosts. A normal invocation reports
available replacements; `--apply` explicitly permits adoption of the selected batch.
Source-specific fetchers retain physical parsing and normalization ownership. #to-review

The updater handles NRCan ratings, existing registered Ontario years, StatCan WDS
metadata/archive pairs, the scenario-selected CER edition, and FuelEconomy's shared
vehicle/range ZIP. The TC HTML policy permits a revised page only when the exact old
registered snapshot proves unchanged parsed market shares, charger counts and reporting
periods. New years/editions, changed coverage or layouts, manual assumptions, reviewed
mapping promotion and external-model generation remain separate review operations. #to-review

All selected candidates are validated before adoption. Revision-specific cache paths
retain old files; one atomic registry replacement activates the validated selection.
Ontario, StatCan and CER request contracts replay adopted snapshots with their registered
hashes. Each run persists a log, JSON audit and exact registry diff through the
`source_update_logs` and `source_update_audits` routes, including skipped sources and
failures. These reports are review evidence, not ETL readiness inputs. #to-review

`.gitattributes` establishes portable text/script line endings and binary preservation,
with specific exceptions for checksum-registered legacy inputs. It does not normalize
registered artifacts during source refresh or make Windows/Excel-only generation
available on other platforms. #to-review

## Diagnostic and research evidence

Mapping bootstrap and `--mapping-diagnostics` are opt-in development paths; road lifetime
generation defaults to accepted evidence, with MTO review enabled by `--mto-diagnostics`
or `--all`.

Single-edition Ontario replay publishes its audit files under `editions/<year>/` and
preserves the aggregate history manifest, status, quality and key-inventory products.
Full-history preparation is explicit through `fetching.vehicle_population --no-download`
without `--year`. Historical consumers validate registered edition coverage and normalized
file hashes before reading it. The vehicle notebook also needs mapping and MTO diagnostics;
the notebook preparation preset produces those products explicitly. #to-review

The stock and efficiency notebooks share the opt-in `notebook_diagnostics.yaml` preset
and native builder. It prepares historical/mapping/retention review evidence and the
diagnostic-only MTO rating benchmark, then captures their exact compressed CSVs and
configuration through `validation.notebook_evidence`. A latest-successful index advances
only after database publication. Readers verify and load the recorded bundle, including
its build identity and database hash, without running ETL or selecting another scenario.
Missing, changed or backend-incompatible captures fail with a rebuild instruction; the
reader never substitutes mutable live configuration. #to-review

Ordinary builds preserve these snapshots and do not require notebook evidence. Only
bundles referenced by the latest index are retained after successful publication; these
generated files are local and excluded from Git. Production builders remain the authority
for parameter rows; diagnostic views and the benchmark do not replace their accepted
values or promote alternative mappings/powertrain weights. #to-review

The transport-comparison notebook reads the database/report selected by
`config/parameters/transport_comparison.yaml`; `legacy_comparison.yaml` prepares its
separate comparison build. Interpret differences against the configured units, keys,
tolerances and explanation evidence, not as automatic baseline acceptance. The growth
notebook uses the fingerprinted products of `parameterization.road_adoption_constraints`;
its fitted rates, seeds and envelopes remain provisional research outside production
scenario wiring and SQLite insertion. #to-review

The growth notebook still binds the removed `legacy_reproduction.yaml` scenario. Its
scenario route needs repair before that research workflow can be opened; it is not part
of the supported stock/efficiency loading commands. #to-review

Supported commands regenerate notebook evidence from registered/reviewed inputs. Agent
judgments, acceptance captures, screenshots and probes are disposable development work
in ignored `tmp/`, not notebook handoff inputs or source provenance. #to-review

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
