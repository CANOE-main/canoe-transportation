---
title: CANOE-main orchestrator integration context
role: Verified upstream integration context for shaping transportation outputs and the future sector adapter.
retrieve_when: A task affects CANOE-main integration, sector-module contracts, shared-database assembly, inherited base configuration, cross-sector commodities, fuel imports, or the transportation adapter.
read_scope: Read this document first, then only the referenced upstream files needed for the changed seam.
verify: CANOE-main is WIP. Re-check the upstream branch and relevant interfaces before implementation; current upstream code outranks this snapshot.
upstream_repo: https://github.com/CANOE-main/CANOE
upstream_branch: yep/base-and-commercial
upstream_commit_reviewed: ad14685837d4989e935aa1ff20a5af881a51ad44
reviewed_on: 2026-09-03
---

# CANOE-main orchestrator integration context

## Purpose

`canoe-transportation` remains a standalone, reproducible backend because its own SQLite
build is needed for legacy parity, focused validation, and independent transport research.
It is also being shaped so the same transport parameterization can later run as a sector
module inside the broader CANOE compiler.

This document records the **currently verified** upstream integration seam. It is not a
promise that the unfinished CANOE-main API will remain unchanged.

## Verified upstream execution model

At the reviewed commit, CANOE-main does **not** merge independently compiled sector
databases. `canoe.pipeline` first calls the base initializer to create one shared
`canoe_schema` v4 SQLite database, then resolves configured sector modules and invokes each
sector's `run()` method against that database. Representative-period processing and TEMOA
execution occur after sector compilation.

The current sector interface is small:

```python
class CANOEModule(ABC):
    def run(self) -> CANOEModuleOutput: ...
    def get_dataset_code(self) -> str: ...
```

`CANOEModuleOutput` currently communicates only `fuel_imports` as a cross-module/global
effect. There is no verified upstream contract in which a sector returns
`dict[str, pd.DataFrame]`, `get_technologies()`, `get_commodities()`, or a separate
`validate()` result.

Sector configuration is a discriminated Pydantic union. The implemented commercial
config uses `module_name = "commercial"`, inherits from `InheritsFromBase`, and resolves
shared fields such as periods, provinces, database path, cache configuration, and data
version from the base compiler configuration. Adding transportation will eventually
require a transport config to be registered in that union or whatever replaces it.

The current commercial builder is the main implemented integration example. It opens an
atomic transaction on the shared database, validates base-owned structure, loads and
transforms sector data, and returns `CANOEModuleOutput`. Use it to understand the current
execution/configuration seam while re-checking upstream code before integration-sensitive work.

## Schema and database seam

CANOE-main and this repository both depend on `canoe-schema`. The upstream initializer
creates the v4 schema with `get_sql_schema("4.0")` and uses `canoe_schema.v4_0` row models
for typed inserts.

`canoe/temoa_protocol.py` is currently the TEMOA execution/configuration adapter. It is
**not** the schema-validation or sector-insertion interface.

For transportation, keep the existing trust boundary:

```text
normalized source evidence
        ↓
transport parameterization
        ↓
parameter-ready artifacts / row-builder outputs
        ↓
canoe_schema row validation + provenance
        ↓
one of two assembly clients
   ├─ build_transport.py        → new standalone transport SQLite
   └─ CANOE-main adapter        → existing shared CANOE SQLite
```

Parameterization modules should therefore not own SQLite connections, transactions, or
CANOE-main lifecycle calls. The parameterization-to-assembly contract should remain stable and testable so both assembly
paths can consume and validate the same transport logic at the schema seam.

## Parameterization shape

Prefer boundaries that correspond to materially different modeling behavior, data
contracts, or transformation chains rather than one module per final SQLite parameter.

For transportation, **road versus off-road is a reasonable middle-level seam** where the
source evidence, capacity representation, units, aggregation, and technology behavior are
materially different. Examples include road/off-road efficiency, cost, lifetime, and
stock/demand transformations.

Do not turn this into a mechanical matrix. Keep a shared module where the behavior is
genuinely shared, and use behavioral modules such as EV infrastructure, LDV charging
profiles, road aggregation, market constraints, or adoption constraints where those are
the clearer ownership boundaries.

A model-facing builder or adapter should compose these transformations into coherent
transport representations. Generic validated insertion remains shared infrastructure.

## Standalone and integrated modes

The standalone build remains first-class:

- creates and publishes its own transport SQLite;
- preserves legacy comparison and transport-only validation;
- remains reproducible through the local YAML/Snakemake control layer.

The future integrated path should be thin:

- receive or resolve CANOE-main base configuration and the shared database target;
- translate only the configuration fields that cross the repository boundary;
- call the same parameterization and provenance/insertion contracts used by the standalone build;
- validate prerequisites owned by CANOE-base before transport writes;
- return the upstream `CANOEModuleOutput` (or its future replacement).

Do **not** fork parameterization logic into a CANOE-main-specific implementation.

## Configuration implications

Do not replace the transport YAML control layer with CANOE-main TOML during MVP work.
The two systems currently serve different scopes.

When integration is implemented, explicitly classify settings as:

- **CANOE-main/base-owned:** shared database target, model regions/provinces, global periods,
  and other global compiler fields;
- **transport-owned:** transport sources, harmonization rules, scenario switches,
  representation choices, mappings, thresholds, and transport-specific validation;
- **adapter-owned:** only the translation or inheritance needed to reconcile the two
  configuration contracts.

Avoid two independent authorities for the same setting.

## Cross-sector commodities and fuel coupling

The broader CANOE documentation establishes Fuel and Electricity as linker modules, and the
current pipeline reserves `CANOEModuleOutput.fuel_imports` for global fuel effects. However,
the implementation is not mature enough to freeze the transportation coupling API:

- pipeline handling of `sector_output.fuel_imports` is still TODO;
- `commercial/fuel_imports.py` is empty;
- `distribution/fuel/` currently contains a commodity list but no completed Python builder;
- `common/naming.py` currently standardizes dataset codes only, not technology/commodity names.

The fuel list is useful naming evidence: it already contains transportation commodities such
as `T_gsl`, `T_dsl`, `T_h2`, `T_jtf`, and others. Treat those labels as upstream evidence to
reconcile before integration, **not yet as a stable API**.

Keep standalone boundary/import technologies isolated from transport parameterization so
they can be reconciled cleanly with the shared linker contract when that interface is mature.

## Data-cache relationship

CANOE-main has a `GoldConnector`, but it currently serves processed **Silver** cache
artifacts to sector modelers; commercial loaders read those cached files and still perform
sector-specific cleaning and aggregation.

The transport backend may continue to own its validated acquisition/cache pipeline for
sources that are not yet supplied by the common data lake. Do not migrate transport fetching
to the common connector merely for architectural symmetry. Reuse the common cache only when
it provides an equivalent, provenance-preserving source contract.

## Upstream files to inspect before integration-sensitive edits

Use the smallest relevant subset:

- `src/canoe/pipeline.py` — compiler order and sector invocation;
- `src/canoe/common/module_interface.py` — current sector lifecycle/output contract;
- `src/canoe/common/module_inheritance.py` — base-config inheritance mechanics;
- `src/canoe/sector_config.py` — sector registration and TOML resolution;
- `src/canoe/initializer.py` — shared database and base-owned rows;
- `src/canoe/commercial/config.py` and `src/canoe/commercial/build.py` — first sector example;
- `src/canoe/commercial/validation.py` — prerequisite validation pattern;
- `src/canoe/common/db_tools.py` — shared transaction behavior;
- `src/canoe/common/fuels.py`, `src/canoe/common/naming.py`, and
  `src/canoe/distribution/fuel/` — coupling/naming evidence;
- `docs/what_is_canoe/model_architecture.md` — intended multi-sector model framing.

Always verify the upstream branch head before relying on this list.

## Integration-oriented acceptance direction

Before transportation is wired into CANOE-main, the backend should demonstrate that:

1. the standalone build still reproduces the accepted transport baseline;
2. parameter transformations can run without creating or owning a SQLite database;
3. the standalone builder and a thin test adapter can consume the same parameter-ready
   contracts;
4. the adapter can target a pre-initialized v4 database without duplicating base-owned rows;
5. transport-specific provenance and validation survive both assembly paths;
6. cross-sector fuel/electricity labels are reconciled against the then-current linker
   contract rather than guessed in advance.
