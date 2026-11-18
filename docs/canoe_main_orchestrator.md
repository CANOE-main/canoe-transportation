---
title: CANOE-main framework and transportation integration context
role: Commit-pinned upstream lifecycle, object composition, and adapter readiness context.
retrieve_when: A task affects the CANOE-main sector lifecycle, shared database, configuration inheritance, fuel coupling, or transportation adapter.
read_scope: Re-check the supplied branch head, then retrieve only the affected files in the retrieval map.
verify: Current executable contracts outrank this point-in-time assessment; local seam tests do not establish multi-sector integration.
upstream_repo: https://github.com/CANOE-main/CANOE
upstream_branch: unstable
upstream_commit_reviewed: be2235d6fa7275d0a183fe5f89c54b88d67f0f35
reviewed_on: 2026-11-16
local_commit_reviewed: 1689c87e9cc25915b76b36578894c5725b8ed595
---

# CANOE-main framework and transportation integration context

## Evidence and readiness

The user-selected `unstable` branch resolved through the GitHub branch API to
[`be2235d6`][snapshot] on 2026-11-16 (commit dated 2026-11-11). The complete
[comparison from the preceding `2c05f0af` review][upstream-comparison] contains two
commits: fuel-price source overrides (`fcc73b51`, PR #44), and projection-source
interfaces plus ghost modules (`be2235d6`, PR #48). The local reference is
`1689c87e`; subsequent working-tree evidence is separated below into Plan 176 and
Plan 177. Earlier assessments and their build/test counts do not establish acceptance
of the changed Plan 177 baseline.

Plan 177 adopts a changed charger production method. Its bounded clean aggregate and
explicit native builds, final explicit rebuild and serial canonical evidence refresh
have passed. The older exact comparisons below remain explicitly pre-177 evidence;
the current checks and their limits are recorded separately.

The implementation now registers commercial, agriculture, industry and residential
as demand sectors, with electricity and fuel supply as separate compiler stages.
Transportation exists in the sector enum and fuel naming/pricing contracts. A newly
registered generic ghost configuration can reserve and supply its fuel endpoints ahead
of a separately inserted end-use tree. There is still no full transport configuration
or adapter invoking this backend. Ghost endpoint creation does not prove a connected
transport tree, scientific completeness or transportation integration readiness.

This refresh inspected the branch response, complete commit comparison and current
commit-pinned pipeline, initializer, ghost config/build, module interface, projection
and fuel-price owners and dependency pins through GitHub. Unchanged owners in the
retrieval map retain the previous source assessment; selected changed test patches
were reviewed without executing them. Local preparation/insertion/schema interfaces
were checked against the current checkout.
No upstream compiler, data-lake acquisition, solver or combined transport build was run.
The local callable seam is useful and tested independently; **a CANOE-main adapter is
not yet integration-ready**. Concrete shared-row, geography, schema-extension, fuel,
emissions and representative-time contracts remain unresolved below. No adapter or
upstream code is changed by this refresh.

### Transport integration interfaces

The GitHub branch API still resolves `unstable` to [`be2235d6`][snapshot].
The typed [sector registration][registration] has no transport configuration.
The [upstream schema pin][upstream-schema] and [transport pin][local-schema]
share the v4 row model file. Upstream changes bulk SQL handling and the quoting
of several DDL defaults. The caller still needs to verify `technology.notes`,
the shared label policy and final validation in the upstream-initialized database.

This transport pass removed disconnected declarations and unsupported lifetime
keys, used distinct explicit charger group names, and supplied a full-year
calendar for unprofiled standalone databases. The pinned Temoa validator
constructed Ontario fixed/curve and explicit/aggregate alternatives across the
tested cases, including one Ontario profiled period on all 8,760 hourly slices.
The checks covered transport construction. CANOE-main still needs an adapter,
connected supply, final time weights and an integrated optimization fixture.

## Implemented orchestration and ownership

[`pipeline.py`][pipeline] implements the following lifecycle:

1. `initializer.run(base)` creates a database from packaged schema v4.0; it **removes
   an existing database at the configured path**, then commits regions, full-year daily/
   hourly slices, existing/model periods and global discount/loan rates. It appends the
   end-of-horizon marker `future_periods[-1] + period_step`; sector model periods exclude it.
2. Central emissions initialization registers gases, `co2e` and optional emission costs
   in its own transaction, before sector technologies write activities.
3. The ordered `compiler.sectors` mapping runs each typed sector configuration's `run()`.
   The [discriminated union][registration] contains commercial, agriculture, industry,
   residential and generic ghost modules. It has no full transport branch or general
   plugin registration mechanism. Config validation now requires one module per end-use
   sector, based on each module's `get_sector()` identity.
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

[`CANOEModule`][interface] requires `run() -> CANOEModuleOutput`,
`get_dataset_code() -> str` and, newly, `get_sector() -> CANOESector`. Outputs contain
`fuel_imports` and `emissions`.
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
sector overrides equal the base scope. The new [projection interface][projections]
uses typed `gdp` (`cer` or `csv`) and `population` (`statcan` or `csv`) selections,
with `demand_projection_point`; explicitly supplied `gdp_scenario` and
`gdp_projection_point` now fail with migration guidance. CSV series use
`region, year, value` and optional `source`, with positive values, duplicate rejection
and province-specific or national series coverage. Transport's accepted demand method
does not automatically become this upstream growth method through field inheritance.

Transport YAML continues to own its source editions, modeling selections and artifact
contracts. A future adapter should translate only agreed inherited fields: database,
region meaning, periods/horizon, currency/rates and relevant shared conventions. It must
reject incompatible settings rather than quietly replace transport selections, reuse
stale files or introduce another transport configuration authority. The data-lake cache
is an upstream module dependency, not a reason to replace registered transport inputs or
permit acquisition during offline replay. Upstream `force_sync` currently raises
`NotImplementedError` before initialization; this is not a supported refresh path.

### Ghost endpoint reservation

[`CANOEGhostConfig`][ghost-config] supports an inline transportation entry with
`module_name = "ghost"`, `sector = "TRP"`, an explicit `fuels` list and inherited
database, provinces and data version. [`build_ghost`][ghost-build] registers one
sector-wide dataset and its fuel commodities, with electricity balanced per time slice
and other fuels annually, then returns fuel-import declarations for those provinces.
It builds no transport technology, demand, profile or transport emissions. Its source
explicitly states that Temoa drops the reserved supply as demand orphans until an
end-use tree consumes it.

This is a useful staged integration route for Yamil, provided actual prepared transport
inputs determine the fuel list and the tree is inserted before representative-period
reduction or solving. The current compiler has no post-supply transport insertion hook;
using the ghost requires explicit external sequencing or a future lifecycle hook.
A full transport module and its ghost cannot both occupy the transportation sector in
one compiler configuration. Dataset/commodity ownership and emissions finalization
must be reconciled when replacing the ghost or inserting after compiler finalization.
If later insertion adds `emission_activity` rows, the first central finalization has
already happened. Calling it again unchanged rejects previously derived `co2e` rows.
Such an integration needs insertion before that first finalization or an explicitly
owned recomputation contract; a second call alone is not an emissions refresh.

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
database**. The pre-177 commands, evidence and omissions are recorded in
[ExecPlan 176](../.agents/plans/176_production_integration_readiness.md). Current
Plan 177 verification is reported separately; historical counts do not establish it.

The pre-177 Plan 176 installed-distribution check found and repaired two collaborator-facing
defects: the wheel omitted the public `build_transport` module, and prerequisite owners
could reload an explicit configuration bundle against the process working directory.
That wheel contained the 88 Python files in the Plan 176 tree. In a clean runtime-only environment,
an installed caller ran from a foreign working directory without repository imports,
`PYTHONPATH`, default-control escape, precomputed tables or network access. It replayed
prerequisites and prepared/inserted 2,335 rows in 17 tables for Ontario, five existing
vintages and one 2025 model period; all 17 support checks, provenance, foreign keys and
integrity passed. The caller's transaction remained active and rollback removed the
inserted technologies. All 227 registered raw files retained their hashes. This is a
bounded real-input caller test, not an upstream-initialized multi-sector fixture.

### Plan 177 changes and verification status

[Plan 177](../.agents/plans/177_charger_production_and_reproducible_evidence.md)
adopts a shared production charger calculation in
[`charger_representation.py`](../src/parameterization/charger_representation.py), reused by diagnostics.
Aggregate LDV/MHDV costs now use evolving Dunsky composition and matched Table 12
DCFC ratings; five explicit archetypes add source-derived utilization and available-GW
composition constraints. The [infrastructure method](model/infrastructure.md) owns
these choices and their proxies, including the scenario home factor 0.08.

The new home-utilization source `T41` still has pending DQ annotations resolved by
registry defaults. iZEV DQ annotations have since been user-reviewed. Explicit
archetype RAMP profiles remain unsupported: select `travel_behavior_source: none`.
Twenty-six obsolete validation outputs were untracked with local copies preserved;
the unused regional/baseline replay routes were removed. Output snapshots are not
production input authority.

**Current Plan 177 scoped native checks passed.** Independent aggregate and explicit
Ontario builds started without derived inputs and with network sockets denied. Both
passed support, foreign-key, integrity, provenance and processed-export checks. The
final explicit rebuild also passed after the parent-group identity correction; it
checked eight groups, twelve members, six shares per period, no hourly capacity
factors and the home-source lineage for all five model periods.

An independent sorted-SQL comparison across 88 tables against the matched Plan 176
Ontario reference found no noncharger numerical or structural differences, excluding
the three provenance registry tables and identifier, source, notes and DQ fields.
The user-reviewed DQ changes are intentional; charger numerical differences are
confined to investment and fixed costs. This comparison does not certify other
scenario selections from the historical matrix.

The serial canonical baseline, calibration, regional and notebook builds and their
five report/support commands completed successfully. Source, default-scenario and
path controls and all 2,256 cache files retained their hashes; four pre-existing
user-modified range tables were restored after immutable evidence capture. The
opt-in annual charger/transport screen passed all five explicit Ontario periods.
It is a necessary annual relaxation, with hourly dispatch, upstream supply and
cross-period investment persistence outside its scope. No full current test suite,
upstream adapter, multi-sector assembly, macOS run or full Temoa solve is established
by these local checks. [Plan 177](../.agents/plans/177_charger_production_and_reproducible_evidence.md)
records the commands, counts and remaining limitations.

Publication rehearsal also repaired shared-year dependencies in the fleet/cohort
diagnostics, an unnecessary LDV dependency in truck-battery evidence, and obsolete
component-audit column names in the comparison notebook. Selected-year weights now
derive from complete captured history and reviewed mappings; consumed truck inputs
retain their year/source checks. The source cache and user-edited range outputs
remain byte-identical. Final focused production/configuration/insertion/DQ checks
pass 172 cases; the installed-wheel check verifies all 90 runtime Python files and
foreign-directory imports, including the optional annual screen without eager SciPy.
The complete 39-figure export and strict 20-page site/link build passed. Local
browser review of charging, infrastructure and derivations passed with 12 rendered
diagrams, current figure hashes and no external requests or rendering errors.
These are publication checks; they do not add a solver or upstream assembly claim.

### Collaborator reproduction boundary

Use the repository's Python 3.13 selection (`.python-version`) and versioned lockfile
(`uv sync --locked`) for this backend's environment. Its declared requirement is
`>=3.13`, versus upstream's `>=3.12`. Every native
scenario build replays prerequisites; an existing processed table is not readiness
evidence. `--download-sources` permits acquisition of missing registered caches, while
reviewed manual inputs and registered external-model outputs are required where
the scenario selects them. The source updater's report-bound candidate adoption is a separate review
operation and must not be silently substituted for pinned scenario replay.

`config/parameters/rules.yaml` is now a routing index. Supply the complete configured
parameter directory, including all six declared partitions; the unchanged owner-lookup
API resolves them independently of the caller's working directory. Native reports and
reviewed source updates pin every member, and captures bind verified recorded members
before configuration validation. The [parameter instructions](../config/parameters/AGENTS.md)
and [Plan 178](../.agents/plans/178_parameter_contract_partitions.md) record the layout
and exact payload/provenance regression evidence. Historical v2 captures remain readable;
older source-update review reports require regeneration before adoption.

The pre-177 Plan 176 pass on November 16 acquired all 19 then-active upstream-download
families; that registry also had ten reviewed manual sources and four external-model
families requiring registered model outputs. Its empty-cache default native build
downloaded 210 files (537,384,857 bytes), then published 385,512 transport rows in 27
tables with integrity/provenance OK, zero FK violations and all 17 support checks.
At zero tolerance, all 88 model tables matched the frozen Plan 176 reference, including
DQ and excluding the three provenance registry tables and `data_id`, `data_source`,
`notes`. Acquisition success and this pre-177 regression comparison are separate
evidence; neither establishes parity after Plan 177 changes charger parameters.

Plan 176 completed independent native rebuilds of its seven presets and six additional
method selections. That pre-177 matrix covered Advanced/Conservative/Mid ATB trajectories,
fixed NHTSA and MOVES/survival alternatives, CEUD calibration/extension choices,
lightweight embodied materials, range representation, and all six NHTS/TTS charging
profiles. Its aggregate charger path used AFDC and StatCan 23-10-0308-01. The
28 equivalent fetched replacements and 20 retained manual rows describe that earlier
input-preservation step, superseded by Plan 177's adopted production method above. Global ATB
`Constant` and schema-incompatible VKT age schedules are rejected before production I/O;
they are not stable scenario alternatives. The three published vocational-class
Constant-to-Conservative exceptions preserve native trajectory labels and source rows.

Fresh-checkout tests uncovered a separate RAMP identity defect: stored Git blobs had
LF endings but the registered producer hashes described CRLF files. Specific attributes
now reproduce the authenticated bytes for the six profiles and six metadata files.
Tests exercise the actual stored index/HEAD blobs and all three `core.autocrlf` settings;
they do not substitute convenient working-copy bytes. Registered raw evidence is unchanged.

Normal GREET consumption is independent of Excel: it authenticates both registered
workbooks, the complete result-bank manifest/report/tables, generation profile and ATB
archetype evidence before normalization. `uv run python -m fetching.greet_automation
--scenario config/scenarios/default.yaml --validate-only` validates that input package without
writing files. A collaborator needs the complete registered package, including the
workbooks; sharing only a normalized CSV does not satisfy the provenance contract.

Fresh GREET recalculation currently uses desktop Excel through a **Windows COM**
worker. The adapter calls native link, calculation and iteration APIs, and the
dependency is selected on Windows only. xlwings' [native API documentation][xlwings-api]
explicitly distinguishes Windows COM from macOS appscript; general macOS xlwings support
does not establish this worker or these GREET VBA workbooks work on macOS. Portable
bank replay and fresh Windows recalculation must be reported separately. Native macOS
GREET regeneration remains unimplemented and unverified; the Windows-generated,
authenticated result bank supports independent scenario rebuilding on other platforms.

On 2026-11-16 a fresh Windows Excel 16.0 / xlwings 0.37.5 worker recalculated all
23 configured cases from disposable copies of the registered workbook pair. All cases
converged in two passes; 966 individual-gas rows, 322 exclusions and 768 independence
comparisons passed validation. All keyed gas and exclusion values exactly matched the
registered bank, with source workbook hashes unchanged and no bank publication.
This establishes fresh calculation on this Windows workstation; it does not establish
native macOS regeneration or independent scientific validity. Commands and timing are
recorded in ExecPlan 176.

## Prior integration gaps reconciled

The IDs below provide a stable crosswalk to the previous document's unresolved topics.

| ID / previous topic | Current evidence and disposition | Remaining requirement / useful next check |
| --- | --- | --- |
| INT-01 — Region meaning | **Still open.** Upstream provinces use `BC`; transport `BCT` includes territories. `NL -> NLLAB` and `PE -> PEI` are naming mappings already supported locally. Upstream's NRCan helper acknowledges BCT source geography but does not change model-region meaning. | Agree geographic coverage and validate regional keys/coverage; a string rename cannot establish physical equivalence. |
| INT-02 — Schema compatibility | **Partly narrowed, still open.** Upstream pins schema [`b8a177bf`][upstream-schema]; local pins [`1e68c377`][local-schema]. Retrieved v4 model files are text-identical. [DDL text][upstream-ddl] differs from the [local pin][local-ddl] only in 21 operator-default quote changes (`"le"` to `'le'`), so commit difference alone is not evidence of broad row incompatibility. Both Technology models lack `notes`; upstream initialization does not apply transport's approved extension. | Agree one packaged dependency/extension contract; test an upstream-initialized connection with required columns, defaults, constraints and unchanged transport notes. Source comparison is not a complete package/runtime compatibility test. |
| INT-03 — Shared structural rows / provenance | **Still open.** Upstream initializes shared gases/`co2e` under electricity datasets. Transport's internal template also defines gases and `co2e`; `co2e` units differ (`kt` versus `ktCO2e`). Commodity keys include `data_id`, allowing same-name definitions under separate datasets. | Explicitly partition/reuse shared structures and audit namespaces; reject conflicting same-name definitions and retain internal structural provenance without fabricated external sources. |
| INT-04 — Electricity supply | **Supply and endpoint-reservation source paths exist; connection unverified.** Electricity supplies sector endpoints such as `T_elc`; fuel supply excludes electricity. A transportation ghost can now reserve these endpoints before the tree is inserted. | Derive the fuel list from actual prepared transport consumption, reconcile the ghost's commodity ownership, and verify charger/blend paths reach supplied electricity before reduction or solving. |
| INT-05 — Fuel blends, hydrogen and emissions | **Still open.** Fuel distribution writes combustion emissions under consuming sectors and imports write upstream emissions. Local blend descriptions and pressure-specific hydrogen inputs do not follow from generic supply declarations. Central finalization covers `emission_activity`, not transport `emission_embodied`. | Resolve blend/conditioning endpoints, emission ownership and gas/GWP treatment without double counting; preserve GREET embodied-emission meaning and DQ. See below. |
| INT-06 — Preparation effects / module registration | **Still open, with a new staged route.** Transport has shared preparation/insertion and full native prerequisite replay; upstream has generic ghost registration but no registered transport adapter or multi-sector integration fixture. `get_sector()` is now required and duplicate sector modules are rejected. Preparation still publishes configured files. | Bound an adapter request or external ghost/tree sequence, validate base compatibility and artifact identity, reconcile shared rows, and exercise a real upstream-initialized connection before registration. |
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
The new [fuel-price source interface][price-sources] accepts an ordered `price_sources`
list (`eia`, `atb`, `fixed` or `csv`) and `price_fallback`, defaulting to `per_series`
for each region/sector/fuel; `per_fuel` selects all prices of a fuel from its first
available source. CSV price rows carry region, sector, fuel, year, price, currency,
currency year and units (`$/GJ` or `$/MMBtu`), with optional source text; empty region
or year expands to the requested coverage with warnings and duplicate checks. Sources
are converted to the model currency year before selection. Coverage checks can reject
or drop unsupported provincial imports according to `missing_data_behavior`; review
that setting before treating a declared endpoint as supplied.

Price routes include transport fuels and fixed renewable components, but the
fuel entities remain efficiency-one transfers. Fixed ethanol/renewable-diesel/SPK
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
5. Register the typed transport config, including `get_sector()`, in the upstream sector union only after this
   fixture passes and a bounded combined compile proves the seam. Do not infer solver
   feasibility, scientific validity or complete integration from schema/fixture success.

No `canoe_adapter.py` is implemented here. Reuse upstream naming, declarations and
transaction interfaces where agreed; retain local source selection, parameterization,
validated insertion and standalone publication. New labeled-array/entity abstractions
are unnecessary unless an actual integration consumer demonstrates a need.

## Upstream retrieval map

All current source links below are pinned to the reviewed commit. For another refresh,
resolve the explicitly selected branch again; record a new pin rather than silently
following moving links. This refresh used the complete two-commit comparison to identify
changed seams; it did not repeat the previous full-tree or all unchanged-file inspection.

| Seam | Inspected owners / evidence |
| --- | --- |
| Lifecycle/configuration | [pipeline][pipeline], [initializer][initializer], [sector registration][registration], [inheritance][inheritance], [ghost config][ghost-config]/[build][ghost-build], [projection sources][projections], residential/electricity/fuel configs |
| Transactions/validation | [db_tools][transactions], [common validation][common-validation], electricity/fuel validation |
| Assembly/provenance | [technology][technology], [parameter metadata][parameters], [commodity entities][commodities], industry/commercial builders; industry structure/entity and residential config tests |
| Supply coupling | [fuel build][fuel-build], [price sources][price-sources], [fuel entities][fuel-entities], [declarations][fuel-declarations], [electricity build][electricity-build], [grid supply][electricity-supply], [naming][naming], [provinces][provinces] |
| Emissions/time/schema | [central emissions][emissions], [representative-period build][representative-build] and reduction/mapping; fuel/grid/emission tests; [dependency pins][dependencies], both pinned v4 models and DDLs |

[snapshot]: https://github.com/CANOE-main/CANOE/commit/be2235d6fa7275d0a183fe5f89c54b88d67f0f35
[pipeline]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/pipeline.py
[initializer]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/initializer.py
[registration]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/sector_config.py
[interface]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/module_interface.py
[inheritance]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/module_inheritance.py
[transactions]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/db_tools.py
[common-validation]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/validation.py
[technology]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/canoe_objects/technology.py
[parameters]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/canoe_objects/parameter.py
[commodities]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/canoe_objects/commodity.py
[fuel-build]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/fuel/build.py
[fuel-entities]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/fuel/entities.py
[fuel-declarations]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/canoe_objects/fuel_imports.py
[electricity-build]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/electricity/build.py
[electricity-supply]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/electricity/supply/entities.py
[naming]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/naming.py
[provinces]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/provinces.py
[emissions]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/emissions/processing.py
[upstream-factors]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/fuel/data/upstream_emission_factors.csv
[representative-build]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/representative_periods/build.py
[dependencies]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/pyproject.toml

[upstream-schema]: https://github.com/CANOE-main/canoe-schema/blob/b8a177bff5032e2404f3f5ea6b807fc964bcf526/canoe_schema/v4_0/models.py
[local-schema]: https://github.com/CANOE-main/canoe-schema/blob/1e68c377d5a7499c78b009d7c472ffd5a6b44901/canoe_schema/v4_0/models.py
[upstream-ddl]: https://github.com/CANOE-main/canoe-schema/blob/b8a177bff5032e2404f3f5ea6b807fc964bcf526/canoe_schema/v4_0/schema.sql
[local-ddl]: https://github.com/CANOE-main/canoe-schema/blob/1e68c377d5a7499c78b009d7c472ffd5a6b44901/canoe_schema/v4_0/schema.sql

[upstream-comparison]: https://github.com/CANOE-main/CANOE/compare/2c05f0afed86efe186b357f989b4c003f00ba65b...be2235d6fa7275d0a183fe5f89c54b88d67f0f35
[ghost-config]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/ghost/config.py
[ghost-build]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/ghost/build.py
[projections]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/projections.py
[price-sources]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/fuel/price_sources.py
[xlwings-api]: https://docs.xlwings.org/en/stable/missing_features.html
