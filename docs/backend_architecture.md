---
title: Backend architecture
role: Structural design reference for repository layout and module ownership.
retrieve_when: A task affects repository structure, module ownership, orchestration seams, or artifact placement.
read_scope: Read only the relevant tree branches and descriptions.
verify: Check planned content against current code, config, tests, schemas, and validation evidence before implementation.
---

## Application and operating model

The backend compiles configured Canadian transport scenarios into CANOE/Temoa v4
SQLite databases. It turns registered downloads, external-model results and
reviewed manual inputs into normalized evidence, model parameters and provenance
records. Standalone publication is atomic at the SQLite file boundary.

The standalone transport SQLite remains a first-class output for scenario comparison, focused
validation, and independent transport research. The backend also exposes contribution
preparation and insertion for a caller-owned compatible database. Both paths use the same
transport parameterization; an upstream-specific adapter remains planned.

The tree maps the implemented entry points and their owners.

```text
.
├── AGENTS.md                                # Stable repository policy
├── README.md                                # Human-facing project orientation
├── .gitattributes                           # Portable text/binary checkout and registered source-byte exceptions
├── .agents/
│   ├── PLANS.md                             # ExecPlan protocol
│   ├── plans/                               # Task-local implementation records
│   └── skills/                              # Optional task-retrieved procedures
├── config/
│   ├── paths.yaml                           # Canonical paths and artifact-family impact routes
│   ├── sources.yaml                         # External-source registry, provenance and explicit update policies
│   ├── scenarios/                           # Scenario authoring contract
│   └── parameters/
│       ├── AGENTS.md                        # Parameter-contract responsibilities and routing
│       ├── rules.yaml                       # Explicit routing index for six rule partitions
│       ├── source_normalization.yaml        # Source layouts, selectors and normalization
│       ├── fleet_stock_and_activity.yaml    # Mapping, stocks, demand, utilization and lifetimes
│       ├── vehicle_efficiency_and_emissions.yaml # Consumption, calibration and vehicle cycle
│       ├── transport_costs.yaml             # Investment and operating-cost conventions
│       ├── charging_and_range_representation.yaml # Infrastructure, hourly profiles and EV ranges
│       ├── transport_diagnostic_controls.yaml # Shared opt-in diagnostic controls
│       └── conversion.yaml                  # Reusable conversion factors
├── src/
│   ├── setup.py                             # Configuration/schema smoke entrypoint
│   ├── build_transport.py                   # Transport contribution and atomic database assembly
│   ├── fetching/                            # Upstream download, cache, and interim normalization
│   │   ├── source_updater.py                # Explicit rolling-source review/adoption, immutable snapshots and diff logs
│   │   ├── nrcan_ceud.py                    # NRCan CEUD tables and checksum-pinned fuel-consumption ratings
│   │   ├── vehicle_population.py            # Ontario MTO report acquisition and normalization
│   │   ├── statcan_tables.py                # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py                # CER energy future tables
│   │   ├── nlr_atb_autonomie.py             # NLR ATB and ANL Autonomie inputs
│   │   ├── greet_automation.py              # Explicit disposable-copy Excel generation of vehicle-cycle evidence
│   │   ├── greet_vehicle_cycle.py           # Registered source contracts, extraction and offline bank validation
│   │   ├── fueleconomy_vehicles.py          # Shared pinned vehicle-class and diagnostic range evidence
│   │   ├── legacy_charging_profiles.py      # Immutable NHTS/TTS charging evidence and legacy hourly transformation
│   │   ├── epa_omega_baseline.py            # Compact legacy OMEGA baseline sales/CD ranges and source identity validation
│   │   ├── technology_growth_evidence.py    # Registered adoption evidence acquisition and normalization
│   │   ├── vpic_vehicle_types.py            # Opt-in vPIC vehicle-type evidence
│   │   ├── vpic_model_years.py              # Opt-in vPIC make/model-year evidence
│   │   └── assorted_sources.py              # Smaller registered source adapters
│   ├── parameterization/                    # Transform normalized inputs into model parameters
│   │   ├── build_existing_capacity.py       # Combine and validate road/off-road capacity rows
│   │   ├── build_demand.py                  # Project and validate combined transport demand
│   │   ├── build_lifetime_parameters.py     # Prepare fixed lifetimes or annual curves with process endpoints
│   │   ├── build_efficiencies.py            # Combine efficiency rows and PHEV input splits
│   │   ├── build_costs.py                   # Combine and validate investment/variable costs
│   │   ├── manual_parameters.py             # Validate registry and resolve generic selectors
│   │   ├── road_stocks_and_demands.py       # Road existing stock and demand products
│   │   ├── ceud_inputs.py                   # Explicit base-year views; preserves native CEUD observations #to-do: determine whether this belongs in param/ or in utils/
│   │   ├── road_utilization.py              # Road C2A and annual utilization, including vintage-period artifacts
│   │   ├── offroad_stocks_and_demands.py    # Off-road existing stock and demand products
│   │   ├── road_lifetimes_survival.py       # Accepted road lifetime, survival, and MTO diagnostics
│   │   ├── road_cohort_survival.py          # Pure annual survival reconciliation and cohort arithmetic
│   │   ├── offroad_lifetimes.py             # Lifetimes of remaining technologies
│   │   ├── road_aggregation.py              # Reviewed mapping application and aggregation weights
│   │   ├── road_fleet_weights.py            # Shared production class/powertrain fleet composition
│   │   ├── vehicle_mapping_bootstrap.py     # Explicit mapping-development entrypoint
│   │   ├── road_efficiencies.py             # Road technology efficiencies
│   │   ├── offroad_efficiencies.py          # Off-road technology efficiencies
│   │   ├── road_capex_opex.py               # Road investment and operating costs
│   │   ├── offroad_capex_opex.py            # Off-road investment and operating costs
│   │   ├── currency.py                      # CER-backed currency and price-year conversion
│   │   ├── road_embodied_emissions.py       # Road vehicle-cycle lifetime gases and regional aggregation
│   │   ├── ev_chargers.py                   # EV charging infrastructure preparation and validated artifacts
│   │   ├── ldv_charging_profiles.py         # Inherited hourly profiles, configured scaling and annual utilization limits
│   │   ├── ldv_ev_ranges.py                 # OMEGA range shares, minimum constraints and parameter-specific representative LDV aggregation
│   │   └── road_adoption_constraints.py     # Opt-in adoption evidence diagnostics and provisional calibration # to-do: pending full implementation; currently beyond MVP
│   ├── utils/
│   │   ├── __init__.py                      # Typed config loading and artifact path resolution
│   │   ├── files.py                         # Shared hashing and atomic CSV/text publication
│   │   ├── build_logging.py                 # Persistent scenario/YAML/UTC-named build logs
│   │   └── vehicle_labels.py                # Shared vehicle-label mechanics
│   └── validation/ # to-do: let's make sure that validation modules are not stale and they maintain alignment with the latest modeling choices and provenance
│       ├── config_models.py                 # Pydantic configuration contracts
│       ├── config_smoke.py                  # Setup-time config/schema status and directory creation
│       ├── provenance.py                    # Source and dataset provenance
│       ├── schema_contract.py               # canoe-schema v4 compatibility
│       ├── insertion.py                     # Scoped pre-insertion cleanup and validated insertion
│       ├── database_bootstrap.py            # Post-insertion database integrity checks
│       ├── sqlite_utils.py                  # Shared SQLite identifier mechanics
│       ├── sqlite_compare.py                # Scenario-to-scenario SQLite comparison
│       ├── transportation_research.py       # Opt-in researcher reference, cohort and closure reports
│       ├── stock_cohort_diagnostic.py       # Read-only sales, stock and survival diagnostics
│       ├── charging_cost_comparison.py      # Opt-in source-year and unit-aware charger cost comparison
│       ├── notebook_evidence.py             # Captured, hash-validated stock/efficiency evidence bundles
│       ├── vehicle_stock_diagnostic.py      # Reader and views for recorded stock/mapping/retention evidence
│       ├── ldv_efficiency_diagnostic.py     # Reader and views for recorded efficiency evidence
│       ├── charger_feasibility.py          # Opt-in read-only annual explicit-charger screen
│       └── rating_stock_benchmark.py        # Diagnostic MTO/rating comparison, outside production weights
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
│   ├── validation/                          # Build reports, source-update audits and recorded notebook bundles
│   └── logs/                                # Persistent build attempts and source-update logs
├── docs/
│   ├── backend_architecture.md              # Repository structure and ownership reference
│   ├── etl_flowcharts.md                    # Parameter-specific lineage reference
│   ├── assumptions.md                       # Enduring data challenges and modelling assumptions
│   ├── source_inventory.md                  # Registry-based source uses, formats and footprint snapshot
│   └── insights/                            # Marimo diagnosis notebooks; see diagnostic workflows below
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
| `build_existing_capacity` | Solver capacity rows, physical reference-year stock, annual acquisitions, provenance and reconciliation evidence. |
| `build_demand` | Road/off-road base activity, CER scenario projections, and regional-period demand. |
| `build_lifetime_parameters` | Fixed technology lifetimes, or annual survival and matching process lifetimes for car and truck curves. Other technologies retain their fixed-lifetime treatment. #to-review |
| `build_efficiencies` | Fuel/service efficiency edges and PHEV input splits, gated by existing capacity. |
| `build_costs` | Investment and variable costs, harmonized with `currency` and constrained by capacity/lifetime coverage. |

`road_cohort_survival` owns pure source-tail reconciliation and acquisition-weighted
vintage survival arithmetic. `road_lifetimes_survival` selects registered schedules and extends
the last retirement rate to the scenario survival threshold (0.5% by default), followed by zero. `build_existing_capacity` exposes physical survivors
separately from solver capacity; utilization, efficiency and charger preparation consume
that physical-stock contract. Contribution insertion rechecks annual curve endpoints,
positive annual values and explicit lifetime coverage before writing.

The stock/cohort notebook uses `validation.stock_cohort_diagnostic` to reconstruct
comparisons from registered inputs without publishing production parameters. The notebook
assesses the promoted constant-retirement method and reports its end ages and any
extra retirement after grouping. Curve builds validate annual values and matching
process lifetimes before publication.

`build_transport.py` calls the same preparation functions for standalone and caller-owned
assembly. `prepare_transport_contribution` gathers structural templates, capacity, demand,
road utilization, lifetimes, efficiencies, costs, charger rows, charging profiles,
range-share groups/constraints and embodied emissions as selected.
`insert_transport_contribution` registers provenance and inserts validated rows into a
compatible caller-owned connection. The caller owns shared model axes, existing shared
structures and resources, the transaction, final validation and publication.
The standalone path owns those lifecycle steps for its own database. Schema contracts
and insertion mechanics remain in `validation/`, backed by the pinned
`canoe-schema` package. See the [integration boundary](model/integration.md). #to-review

The pinned package owns both v4 DDL and final row models. Database creation uses
`canoe_schema.get_sql_schema("4.0")`; insertion registers provenance, validates the
corresponding row model, rejects conflicting keys, then checks foreign keys and integrity
before publication.

## Running and extending the backend

The supported scenario command is `uv run python src/build_transport.py --scenario
config/scenarios/default.yaml`; add `--overwrite` to replace its configured database.
It owns the complete production lifecycle through `prepare_scenario_inputs`, followed by
shared contribution preparation, validated insertion and atomic SQLite publication.
Scenario selections stay in YAML. `setup.py` and `scripts/doctor.py` are optional
configuration/schema and readiness checks, not mandatory build stages. See the
[README](../README.md) for installation and the concise operating commands.

Every invocation regenerates CEUD/ratings, StatCan, CER, ATB/Autonomie, assorted sources,
the pinned AFDC station inventory, the selected Ontario Report A/4/5 edition and reviewed road aggregation
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
details. Ordinary logs use INFO, with detailed audits enabled by `--debug`.

## Explicit source updates

`fetching.source_updater` is a maintenance entrypoint outside the scenario lifecycle.
The typed `updater` limits and per-source `update` policies in `sources.yaml` determine
eligible public endpoints and allowed download hosts. A normal invocation reports
available replacements; `--apply` explicitly permits adoption of the selected batch.
Source-specific fetchers retain physical parsing and normalization ownership.

The updater handles NRCan ratings, existing registered Ontario years, StatCan WDS
metadata/archive pairs, the scenario-selected CER edition, and FuelEconomy's shared
vehicle/range ZIP. Review proposed changes and captured responses before adoption;
reviewed apply replays those exact responses offline and rechecks registry, scenario,
the rules index and every declared partition, and source selection. Retain the complete
report directory until adoption. Source-update review-schema 2 reports require a fresh
review before adoption; schema 3 explicitly pins the full rule dependency set.
Unknown old-cache contents do not establish unchanged values.

AFDC acquisition validates the registered Canadian public/open snapshot before
deriving provincial L1/L2/DCFC counts. Native prerequisites also replay the pinned
Dunsky report using the registered table selectors. Both the aggregate and
explicit production representations use this extraction, with supplemental
reviewed rows and aggregate utilization overrides. The explicit branch emits
charger-type structure and period capacity-share constraints. It requires
unprofiled charging. See [charger derivations](etl_flowcharts.md#ev-charger-parameters).
Mapping changes and external-model generation retain separate review boundaries. #to-review

NRCan and FuelEconomy model-year additions and StatCan reference-date extensions can
produce reviewed proposals while scenario selections stay unchanged. Schema/frequency
changes and loss of registered coverage remain errors. FuelEconomy reports distinguish
ZIP packaging changes from consumed-record changes, retaining multiset hashes, counts
and bounded added/removed examples. Missing optional publication headers use a stable
content identity with explicit review. A validated registered StatCan archive also
protects its interior reference periods; absent historical archives are not evidence
of complete period preservation.

All selected candidates are validated before adoption. Revision-specific cache paths
retain old files; one atomic registry replacement activates the validated selection.
Ontario, StatCan and CER request contracts replay adopted snapshots with their registered
hashes. Each run persists a log, JSON audit and exact registry diff through the
`source_update_logs` and `source_update_audits` routes, including skipped sources and
failures. These reports are review evidence, not ETL readiness inputs.

Captured response files live inside the existing source-update audit route. Their
hashes and relative locations are bound to the report fingerprint; missing, altered or
outside-directory files block apply. The apply audit retains its own copies before
immutable cache/atomic registry publication.

`.gitattributes` establishes portable text/script line endings and binary preservation,
with specific exceptions for checksum-registered legacy inputs. It does not normalize
registered artifacts during source refresh or make Windows/Excel-only generation
available on other platforms.

GREET keeps one calculation authority: the explicit Windows desktop Excel producer,
which drives the registered workbook pair through Windows COM and native VBA. For
occasional workbook, extraction-control or ATB-archetype changes, run
`uv run python -m fetching.greet_automation --scenario config/scenarios/default.yaml`
on a maintained Windows Excel workstation. Review the retained calculation evidence
and distribute the complete registered result bank with its manifest and matching
registered inputs. Ordinary builds on Windows, macOS and Linux consume that handoff
without Excel; a remote calculation service is unnecessary for this workflow.

Use `uv run python -m fetching.greet_automation --scenario config/scenarios/default.yaml --validate-only`
to verify the bank without recalculation or file writes. Historical bank hashes accept
only authenticated uniform LF/CRLF representations, preserving the original manifest
and reporting actual versus retained digests. New explicit publications use LF bytes.
ATB replay supports both CSV record terminators while preserving quoted field content.
Changed values, stale workbook/profile evidence and invalid bank contents still fail.
The portability CI exercises fixture-based checks on Windows, Ubuntu and macOS; it does
not recalculate GREET or establish scientific model equivalence.

## Diagnostic and research evidence

Mapping bootstrap and `--mapping-diagnostics` are opt-in development paths; road lifetime
generation defaults to accepted evidence, with MTO review enabled by `--mto-diagnostics`
or `--all`.

Single-edition Ontario replay publishes its audit files under `editions/<year>/` and
preserves the aggregate history manifest, status, quality and key-inventory products.
Full-history preparation is explicit through `fetching.vehicle_population --no-download`
without `--year`. Historical consumers validate registered edition coverage and normalized
file hashes before reading it. The vehicle notebook also needs mapping and MTO diagnostics;
the notebook preparation preset produces those products explicitly.

The vehicle-population and LDV-efficiency notebooks share the opt-in `notebook_diagnostics.yaml` preset
and native builder. It prepares historical/mapping/retention review evidence and the
diagnostic-only MTO rating benchmark, then captures their exact compressed CSVs and
configuration through `validation.notebook_evidence`. A latest-successful index advances
only after database publication. Readers verify and load the recorded bundle, including
its build identity and database hash, without running ETL or selecting another scenario.
Missing, changed or backend-incompatible captures fail with a rebuild instruction; the
reader never substitutes mutable live configuration.

The [parameter contract instructions](../config/parameters/AGENTS.md) route the six rule
domains and specialized extraction/conversion/review files. The typed rules index
declares one partition per executable owner; missing files, undeclared members and
duplicate ownership fail before production I/O. Consumers keep the same owner lookup
API. Captures authenticate the index and every declared member, then bind the verified
captured parameter directory before configuration validation. Explicit historical
version-2 monoliths stay within their recorded directory. Physical partition changes
require fresh reports/captures through their producer.

`config/parameters/vehicle_reference.csv` and `vehicle_source_links.csv` are the two
versioned vehicle authorities. One read-only loader validates their scoped facts and
source links before mapping and matching. Builds calculate consumption/range summaries
downstream and never regenerate these configuration tables; notebook bundles capture
the same tables once as configuration.

Ordinary builds preserve these snapshots and do not require notebook evidence. Only
bundles referenced by the latest index are retained after successful publication; these
generated files are local and excluded from Git. Production builders remain the authority
for parameter rows; diagnostic views and the benchmark do not replace their accepted
values or promote alternative mappings/powertrain weights.

The transportation validation notebook reads locally rebuilt reports for the current
scenario alongside tracked historical excerpts from
`inputs/validation/transportation_reference/`. The excerpts supply diagnostic evidence
for fleet and charger comparisons.

The growth notebook uses fingerprinted products of
`parameterization.road_adoption_constraints` through `default.yaml`. Its fitted
rates, seeds and envelopes remain provisional research outside SQLite insertion.

Supported commands regenerate notebook evidence from registered/reviewed inputs. Agent
judgments, acceptance captures, screenshots and probes are disposable development work
in ignored `tmp/`, not notebook handoff inputs or source provenance.

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
