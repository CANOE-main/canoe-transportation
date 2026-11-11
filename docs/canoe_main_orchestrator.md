---
title: CANOE-main framework and transportation integration context
role: Commit-pinned upstream lifecycle, object composition, and adapter readiness context.
retrieve_when: A task affects the CANOE-main sector lifecycle, shared database, configuration inheritance, fuel coupling, or transportation adapter.
read_scope: Re-check the supplied branch head, then retrieve only the affected files in the retrieval map.
verify: Current executable contracts outrank this point-in-time assessment; local seam tests do not establish multi-sector integration.
upstream_repo: https://github.com/CANOE-main/CANOE
upstream_branch: unstable
upstream_commit_reviewed: 2c05f0afed86efe186b357f989b4c003f00ba65b
reviewed_on: 2026-11-04
local_commit_reviewed: aafdd708e54840a13188b82c3f80e62fa83ff0fc
---

# CANOE-main framework and transportation integration context

## Evidence and readiness

The user-selected `unstable` branch resolved through the GitHub branch API to
[`2c05f0af`][snapshot] on 2026-11-04 (commit dated 2026-10-29). This replaces the
October `yep/industry-refactor` assessment at `93241c33`; that earlier branch and its
claims remain historical context in [ExecPlan 060](../.agents/plans/archive/060_industry_orchestrator_and_structural_refresh.md).
The local reference is `aafdd708` plus the documentation/reference changes recorded in
[ExecPlan 137](../.agents/plans/archive/137_diagnostic_integration_and_scenario_refresh.md).

The implementation now registers commercial, agriculture, industry and residential
as demand sectors, with electricity and fuel supply as separate compiler stages.
Transportation exists in the sector enum and fuel naming/pricing contracts, but has no
registered sector configuration or implementation. This is broader implemented coverage
than the previous reference; it does not prove scientific completeness, production
maturity or transportation integration readiness.

This assessment inspected commit-pinned source, configurations and selected upstream
tests through GitHub, plus the current local preparation/insertion/schema interfaces.
No upstream compiler, data-lake acquisition, solver or combined transport build was run.
The local callable seam is useful and tested independently; **a CANOE-main adapter is
not yet integration-ready**. Concrete shared-row, geography, schema-extension, fuel,
emissions and representative-time contracts remain unresolved below. No adapter or
upstream code is changed by this refresh.

## Implemented orchestration and ownership

[`pipeline.py`][pipeline] implements the following lifecycle:

1. `initializer.run(base)` creates a database from packaged schema v4.0; it **removes
   an existing database at the configured path**, then commits regions, full-year daily/
   hourly slices, existing/model periods and global discount/loan rates. It appends the
   end-of-horizon marker `future_periods[-1] + period_step`; sector model periods exclude it.
2. Central emissions initialization registers gases, `co2e` and optional emission costs
   in its own transaction, before sector technologies write activities.
3. The ordered `compiler.sectors` mapping runs each typed sector configuration's `run()`.
   The [discriminated union][registration] contains commercial, agriculture, industry
   and residential. It has no transport branch or general plugin registration mechanism.
4. Optional electricity supply receives the demand sectors' imports and supplies their
   electricity, including the sector delivery endpoints. Its generator fuel imports join
   the subsequent fuel-supply requests. Missing electricity configuration with electricity
   imports produces a warning, not a connected supply chain.
5. Optional fuel supply merges all imports by sector/fuel/province, excluding electricity,
   and builds import/distribution chains. Missing fuel configuration also warns.
6. Central emissions finalization checks declared gas activities and derives flow-level
   CO2-equivalent activities. Optional representative-period processing creates a reduced
   database copy; Temoa runs on the selected full or reduced database afterwards.

[`atomic_transaction`][transactions] opens one connection with foreign keys enabled,
commits successful blocks, rolls back ordinary exceptions and closes the connection.
Sector, electricity and fuel builders own separate transactions; entities accept their
connection and do not commit. **This is not one transaction or atomic publication across
the compiler.** Later failure does not restore an earlier database removed by initialization
or roll back already committed sectors. Transport's standalone staged publication must
remain independently usable; integration must respect the module/caller transaction rather
than wrap standalone database creation.

[`CANOEModule`][interface] requires `run() -> CANOEModuleOutput` and
`get_dataset_code() -> str`. Outputs contain `fuel_imports` and `emissions`.
`CANOEFuelImport` identifies a sector, a fuel and consuming provinces;
`CANOEEmissionDeclaration` identifies a sector and gas. Electricity and fuel configuration
objects have `run(fuel_imports)` signatures and are outside the demand-sector union.

## Configuration and parameter assembly

The [base configuration][initializer] owns database location, model/existing periods,
provinces, global rates, currency year, data-version/cache settings and common projection/
emission choices. Sector TOML files or in-memory dictionaries resolve through
[`resolve_sector_config`][registration]. [`InheritsFromBase`][inheritance] fills omitted
or explicitly `"inherited"` fields through validation context; explicit sector overrides
remain possible. Resolvers handle names such as `database_file <- base.db_output_dir`.
An adapter therefore needs validated compatibility checks, not an assumption that all
sector overrides equal the base scope.

Transport YAML continues to own its source editions, modeling selections and artifact
contracts. A future adapter should translate only agreed inherited fields: database,
region meaning, periods/horizon, currency/rates and relevant shared conventions. It must
reject incompatible settings rather than quietly replace transport selections, reuse
stale files or introduce another transport configuration authority. The data-lake cache
is an upstream module dependency, not a reason to replace registered transport inputs or
permit acquisition during offline replay. Upstream `force_sync` currently raises
`NotImplementedError` before initialization; this is not a supported refresh path.

Upstream has useful coherent seams: named calculations, pure entity assembly such as
`industry.entities.build_industry_entities`, and ordered `build(db_conn)` methods.
[`TechnologyEntity`][technology] validates parameter relationships, then builds packaged
row models; [`FuelCommodityEntity`][commodities] and related objects establish endpoints.
Its labeled arrays make region/vintage/period grains explicit. These can inform bounded
local design, but adoption is not required for compatibility.

The existing writers are **not substitutes for transport insertion**:

- Entity writes use insert-or-ignore, which does not reject a conflicting existing row.
- [`RowOptions`][parameters] writes DQ on the first parameter row only; source references
  are optional and default to first-row-only. Commercial source registration remains
  TODO; fuel emission references currently go into notes pending provenance revision.
- Technology writes round fixed lifetimes; the inspected entity has no survival-curve
  composition method, although the packaged schema has the survival table.

Preserve the transport source registry, resolved full DQ/lineage, readable dataset
publication, conflict policy, row validation and support checks. Reusing framework
interfaces does not require converting transport rows to another writer or scientific
representation. Upstream validation helpers check regions, periods, time slices, fuel
endpoints and declarations; these supplement rather than replace local provenance,
expected-key, support, foreign-key and integrity validation.

## Existing transportation callable seam

[`build_transport.py`](../src/build_transport.py) owns the complete native lifecycle:
`prepare_scenario_inputs(bundle, download=False)` replays registered prerequisites, then
`prepare_transport_contribution(connection, bundle=..., template_dir=...)` assembles
`TransportContribution`; `insert_transport_contribution(connection, contribution,
conflict="error")` inserts it. Standalone publication uses the same preparation/insertion
contract and adds staged schema creation, validation and protected publication.

Contribution preparation does not write SQLite rows, but can publish shared interim,
processed and audit files. Integration must call the prerequisite owner as well as
contribution preparation; the contribution call alone is not a complete fresh-input
lifecycle. Different databases or caller transactions do not isolate those shared files.
The [diagnostic snapshot](codebase_diagnostic_snapshot.md) tracks prepared-input identity
and artifact isolation separately from database transaction ownership.

The contribution carries structural rows, demand, capacity, efficiencies, annual PHEV
splits, costs, fixed/survival lifetimes, utilization, charging/range preparation, embodied
emissions, provenance and audits. Its frozen dataclass contains mutable row lists;
insertion rechecks support and representation fingerprints before publication. It
registers provenance first and returns touched row batches. The caller can validate
expected keys, provenance, FKs and integrity with
[`validate_database`](../src/validation/database_bootstrap.py) before committing or
rolling back. It must not reinitialize shared base rows or change caller economics.

Local tests cover a template-only caller-owned transaction/rollback, rejection of an
unextended caller schema, pinned schema contracts and mutable-row support checks.
They use transport-created schema fixtures, **not an upstream-initialized multi-sector
database**. Current focused execution and omissions are recorded in the diagnostic and
ExecPlan 137; historical test counts in the old document are not current results.

## Prior integration gaps reconciled

The IDs below provide a stable crosswalk to the previous document's unresolved topics.

| ID / previous topic | Current evidence and disposition | Remaining requirement / useful next check |
| --- | --- | --- |
| INT-01 — Region meaning | **Still open.** Upstream provinces use `BC`; transport `BCT` includes territories. `NL -> NLLAB` and `PE -> PEI` are naming mappings already supported locally. Upstream's NRCan helper acknowledges BCT source geography but does not change model-region meaning. | Agree geographic coverage and validate regional keys/coverage; a string rename cannot establish physical equivalence. |
| INT-02 — Schema compatibility | **Partly narrowed, still open.** Upstream pins schema [`b8a177bf`][upstream-schema]; local pins [`1e68c377`][local-schema]. Retrieved v4 model files are text-identical. [DDL text][upstream-ddl] differs from the [local pin][local-ddl] only in 21 operator-default quote changes (`"le"` to `'le'`), so commit difference alone is not evidence of broad row incompatibility. Both Technology models lack `notes`; upstream initialization does not apply transport's approved extension. | Agree one packaged dependency/extension contract; test an upstream-initialized connection with required columns, defaults, constraints and unchanged transport notes. Source comparison is not a complete package/runtime compatibility test. |
| INT-03 — Shared structural rows / provenance | **Still open.** Upstream initializes shared gases/`co2e` under electricity datasets. Transport's internal template also defines gases and `co2e`; `co2e` units differ (`kt` versus `ktCO2e`). Commodity keys include `data_id`, allowing same-name definitions under separate datasets. | Explicitly partition/reuse shared structures and audit namespaces; reject conflicting same-name definitions and retain internal structural provenance without fabricated external sources. |
| INT-04 — Electricity supply | **Previous absent-module gap resolved in upstream source; connection unverified.** Electricity now supplies sector endpoints such as `T_elc`; fuel supply excludes electricity. | Declare only actual prepared transport electricity consumption, register the agreed sector endpoint, and verify charger/blend paths reach supplied electricity in an integrated fixture. |
| INT-05 — Fuel blends, hydrogen and emissions | **Still open.** Fuel distribution writes combustion emissions under consuming sectors and imports write upstream emissions. Local blend descriptions and pressure-specific hydrogen inputs do not follow from generic supply declarations. Central finalization covers `emission_activity`, not transport `emission_embodied`. | Resolve blend/conditioning endpoints, emission ownership and gas/GWP treatment without double counting; preserve GREET embodied-emission meaning and DQ. See below. |
| INT-06 — Preparation effects / module registration | **Still open.** Transport has shared preparation/insertion and full native prerequisite replay, but no registered upstream config/adapter or multi-sector integration fixture. Preparation still publishes configured files. | Bound an adapter request, validate base compatibility and artifact identity, then register only a working wrapper around existing owners. |
| INT-07 — Representative-time charging | **More upstream evidence, still open.** Reduction now copies the full database, retains representative CF rows, reweights seasons and demand distributions, and checks FKs/distribution totals. It does not renormalize retained transport CFs to their annual utilization target. | Verify final time weights and weighted utilization/ceilings; an agreed correction belongs in explicit integration behavior, not a silent baseline change. |

### Fuel and emissions endpoints

[`declare_fuel_imports`][fuel-declarations] derives requests from actual technology input
commodities and their regions; it ignores internal or differently named inputs. A
transport equivalent should derive declarations from prepared efficiency edges, including
charger and blend chains. It must not list every commodity in a template. Shared names
follow [`get_fuel_commodity_in_sector`][naming], yielding `T_gsl`, `T_dsl`, `T_h2`, `T_elc`,
etc. The sector must register those consumed commodities before supply validation.

Fuel supply builds `F_ethos -> F_<fuel> -> <sector>_<fuel>`, with import and distribution
technologies; electricity builds its own grid/delivery chain to sector electricity.
Upstream price routes include transport fuels and fixed renewable components, but the
inspected fuel entities are efficiency-one transfers. Fixed ethanol/renewable-diesel/SPK
prices do not implement transport blending proportions or mandates. Local `T_gsl`,
`T_dsl` and `T_spk` descriptions carry blend meanings that cannot be inferred equivalent
from matching names. Do not silently change accepted costs, intensities or emission
accounting when connecting supply.

The generic priced hydrogen route supplies `T_h2`; transport currently consumes
`T_h2_ldv`/`T_h2_mhdv` and defines a 700-bar intermediate. No reviewed connector from
that supply endpoint to those pressure/vehicle endpoints is established. The inspected
[bundled upstream emission table][upstream-factors] has no hydrogen row. Review conditioning,
losses/costs and life-cycle treatment before choosing direct consumption or extra pathways.
These are explicit modeling decisions, not mechanical adapter renames.

Central [`emissions.finalize`][emissions] rejects sector-written CO2-equivalent activity
rows and undeclared gas activities, checks mixed gas units and derives GWP-weighted
`co2e` activities. It does not process `emission_embodied`. Transport currently prepares
GREET embodied emissions separately and does not expose a combustion activity batch in
its contribution. Agree responsibility for every operational and embodied emission
component; returning a declaration alone does not connect or validate a missing pathway.

### Representative-time requirement

Native transport profiles are scaled against the ordinary full-year hourly mean to
retain configured utilization. The upstream reducer retains selected profile values
while changing season frequencies. Thus annual utilization preservation after reduction
is **not established by retained full-year normalization or FK checks**.

For a future integrated fixture, evaluate
`mean_model = sum(weight_slice * CF_slice) / sum(weight_slice)` using season frequency
and slice duration. If integration requires rescaling, explicitly evaluate
`CF_selected * utilization / mean_model`, bounds [0, 1], hourly ceilings, annual factors
and the same time weights before adopting it. Preserve the selected utilization and slack
unless separately authorized. This is a proposed validation/representation requirement,
not a verified or accepted upstream adjustment.

## Bounded adapter requirements and next implementation slice

Keep standalone operation and the existing parameter builders. The smallest useful
adapter slice is a typed request plus an upstream-initialized integration fixture,
followed by a wrapper using these same prerequisites, contribution and insertion owners:

1. Resolve region/period/currency/rate inheritance and schema/notes compatibility before
   preparation. Reject unresolved coverage or conflicting shared structural definitions.
2. Replay registered transport prerequisites offline under an explicit artifact-isolation
   contract; prepare one inspectable `TransportContribution`. Do not invoke the standalone
   publisher or introduce a second preparation implementation.
3. Insert only the agreed transport-owned rows into the module/caller transaction with
   existing conflict, provenance/DQ, support and schema-model checks. Validate touched keys,
   connected endpoints, FKs and integrity before the caller commits; test rollback and
   interruption without claiming whole-compiler publication atomicity.
4. Return upstream fuel/emission declarations from actual prepared pathways under agreed
   ownership. Exercise electricity, blended fuels, hydrogen and embodied/operational
   emissions as applicable. Verify representative-time utilization before solver claims.
5. Register the typed transport config in the upstream sector union only after this
   fixture passes and a bounded combined compile proves the seam. Do not infer solver
   feasibility, scientific validity or complete integration from schema/fixture success.

No `canoe_adapter.py` is implemented here. Reuse upstream naming, declarations and
transaction interfaces where agreed; retain local source selection, parameterization,
validated insertion and standalone publication. New labeled-array/entity abstractions
are unnecessary unless an actual integration consumer demonstrates a need.

## Upstream retrieval map

All current source links below are pinned to the reviewed commit. For another refresh,
resolve the explicitly selected branch again; record a new pin rather than silently
following moving links. The Git tree response was complete (`truncated: false`).

| Seam | Inspected owners / evidence |
| --- | --- |
| Lifecycle/configuration | [pipeline][pipeline], [initializer][initializer], [sector registration][registration], [inheritance][inheritance], residential/electricity/fuel configs |
| Transactions/validation | [db_tools][transactions], [common validation][common-validation], electricity/fuel validation |
| Assembly/provenance | [technology][technology], [parameter metadata][parameters], [commodity entities][commodities], industry/commercial builders; industry structure/entity and residential config tests |
| Supply coupling | [fuel build][fuel-build], [fuel entities][fuel-entities], [declarations][fuel-declarations], [electricity build][electricity-build], [grid supply][electricity-supply], [naming][naming], [provinces][provinces] |
| Emissions/time/schema | [central emissions][emissions], [representative-period build][representative-build] and reduction/mapping; fuel/grid/emission tests; [dependency pins][dependencies], both pinned v4 models and DDLs |

[snapshot]: https://github.com/CANOE-main/CANOE/commit/2c05f0afed86efe186b357f989b4c003f00ba65b
[pipeline]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/pipeline.py
[initializer]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/initializer.py
[registration]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/sector_config.py
[interface]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/common/module_interface.py
[inheritance]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/common/module_inheritance.py
[transactions]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/common/db_tools.py
[common-validation]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/common/validation.py
[technology]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/canoe_objects/technology.py
[parameters]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/canoe_objects/parameter.py
[commodities]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/canoe_objects/commodity.py
[fuel-build]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/fuel/build.py
[fuel-entities]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/fuel/entities.py
[fuel-declarations]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/canoe_objects/fuel_imports.py
[electricity-build]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/electricity/build.py
[electricity-supply]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/electricity/supply/entities.py
[naming]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/common/naming.py
[provinces]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/common/provinces.py
[emissions]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/emissions/processing.py
[upstream-factors]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/fuel/data/upstream_emission_factors.csv
[representative-build]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/src/canoe/representative_periods/build.py
[dependencies]: https://github.com/CANOE-main/CANOE/blob/2c05f0afed86efe186b357f989b4c003f00ba65b/pyproject.toml

[upstream-schema]: https://github.com/CANOE-main/canoe-schema/blob/b8a177bff5032e2404f3f5ea6b807fc964bcf526/canoe_schema/v4_0/models.py
[local-schema]: https://github.com/CANOE-main/canoe-schema/blob/1e68c377d5a7499c78b009d7c472ffd5a6b44901/canoe_schema/v4_0/models.py
[upstream-ddl]: https://github.com/CANOE-main/canoe-schema/blob/b8a177bff5032e2404f3f5ea6b807fc964bcf526/canoe_schema/v4_0/schema.sql
[local-ddl]: https://github.com/CANOE-main/canoe-schema/blob/1e68c377d5a7499c78b009d7c472ffd5a6b44901/canoe_schema/v4_0/schema.sql
