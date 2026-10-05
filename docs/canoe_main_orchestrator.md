---
title: CANOE-main framework and transportation integration context
role: Commit-pinned upstream lifecycle, object composition, and adapter readiness context.
retrieve_when: A task affects the CANOE-main sector lifecycle, shared database, configuration inheritance, fuel coupling, or transportation adapter.
read_scope: Re-check the branch head, then retrieve only the files for the affected seam in the retrieval map.
verify: CANOE-main and this backend are evolving; current code and validated contracts outrank this snapshot.
upstream_repo: https://github.com/CANOE-main/CANOE
upstream_branch: yep/industry-refactor
upstream_commit_reviewed: 93241c33b1a1ca6fbc1bd855008a580da1ae8e2b
reviewed_on: 2026-10-02
local_commit_reviewed: 1bd946bc19a6d8df2cc00331a7f284626d125167
---

# CANOE-main framework and transportation integration context

## Reviewed snapshot

The user-selected reference is `yep/industry-refactor`, reviewed at
[`93241c33`][snapshot] (2026-10-02, “Full industry sector”). It registers commercial,
agriculture, and industry, followed by fuel supply and central emissions processing.
Transportation is still absent from the sector configuration union. The local reference
is `1bd946bc` plus the bounded ownership fixes described in
[the diagnostic](codebase_diagnostic_snapshot.md).

Industry provides the clearest new integration example: calculation modules produce tidy
parameter tables; `build_industry_entities` assembles an inspectable object bundle without
database I/O; the builder registers datasets and writes the bundle in dependency order.
This strengthens the case for retaining the local `TransportContribution` as the transport
assembly object. It does not resolve the remaining shared-database contracts.

This snapshot comes from static inspection of the pinned upstream files and focused local
schema/contribution tests. Upstream review was read-only source/test inspection; no upstream code or data-lake
workflow was executed. A complete CANOE-main run with transportation has not been validated. The backend remains under development: legacy SQLite comparisons may still
change assumptions, parameter coverage, and alignment. Upstream structure informs the
integration interface; it does not accept new transportation modelling choices.

## Tracking a changing development branch

The branch name is a user-supplied retrieval location; the reviewed commit is the evidence
identity. The previous reference was `yep/fuel-refactor` at
`5992b2d1b9884f7f89ed8883854f4dab66fc2cae`. That branch was no longer advertised by
`git ls-remote` during this review. The current checkout includes fuel refactoring at
`7ac064b` and subsequent industry changes; this does not establish a release or merge
schedule.

For another refresh, resolve the explicitly supplied branch with `git ls-remote --heads`,
inspect a temporary checkout at that commit, record the branch/commit/date and relevant
local revision, and replace source links with immutable commit links. Compare the affected
interfaces and trees; do not assume a renamed branch has the same head or commit ancestry.
If the branch disappears and no replacement is supplied, retain the last reviewed pin
as historical evidence and flag the missing reference. Do not silently choose another
development branch or treat `main` as the integration target.

## Operational lifecycle and transaction ownership

The execution order in [`pipeline.py`][pipeline] is:

1. `initializer.run(base)` creates one database with
   `canoe_schema.get_sql_schema("4.0")`, then seeds regions, existing/future periods,
   hourly/daily time slices, and global discount/loan rates. It also inserts an extra
   future period marking the end of the horizon; sectors receive the model periods
   without that final marker.
2. `emissions.processing.init` registers shared gas and CO2-equivalent commodities
   and configured emission costs before sectors write their activities.
3. Each configured sector's typed configuration object runs `run()`. The current
   discriminated union registers commercial, agriculture, and industry. Sector outputs are
   accumulated as `CANOEModuleOutput` objects.
4. The compiler collects all `fuel_imports` and calls
   `compiler.fuel.run(fuel_imports)` when fuel supply is configured. Imports without
   a fuel configuration produce a warning.
5. `emissions.processing.finalize` checks emission declarations against database
   activities and derives CO2-equivalent activities.
6. Optional representative-period processing and TEMOA execution follow compilation.
   CANOE-main assembles sectors into the shared database; it does not merge independently
   compiled sector databases.

The sector contract in [`common/module_interface.py`][interface] remains
`CANOEModule.run() -> CANOEModuleOutput` plus `get_dataset_code() -> str`.
The output now contains both `fuel_imports: list[CANOEFuelImport]` and
`emissions: list[CANOEEmissionDeclaration]`. A fuel declaration carries
`sector`, `fuel`, and a tuple of consuming `provinces`; an emission declaration
carries `sector` and `emission`. Fuel supply has its own `run(fuel_imports)`
signature and is configured separately from the sector union.

Commercial, agriculture, industry, and fuel builders each open [`common.db_tools.atomic_transaction`][transactions]
on the configured database path. It enables foreign keys, commits on success, rolls back
on exceptions, and closes the connection. Entities use `build(db_conn)` inside that
transaction and never commit. Thus the current compiler has separate module transactions,
not one transaction covering all sectors and fuel supply. An adapter should preserve
caller-owned insertion while its CANOE-main module wrapper follows this lifecycle.

## How upstream objects assemble parameter blocks

[`canoe_objects/`][objects] separates reusable model entities from sector calculations.
The useful pattern is to assemble and inspect a coherent object, then build its rows with
an explicit connection. The main contracts are:

| Object or layer | Assembly and operational contract |
| --- | --- |
| `LabeledArray`, `array_types` | Explicit region/period/vintage coordinates; sparse NaN cells are omitted when flattened. Array types express the parameter's grain. |
| `Parameter`, `ParameterMetadata`, `RowOptions` | Values travel with notes, source reference, DQ, and units. Row options determine dataset naming and how metadata is written across rows. |
| `TechnologyEntity` | Fluent `with_*` methods collect efficiency edges, capacity, fixed lifetimes, costs, splits, capacity factors, and emission factors. `validate()` checks consistency; `build(db_conn)` constructs schema row models and writes them. |
| `FuelServingTechnologyEntity` | Composes either a technology per fuel or one shared technology, according to `FuelGrouping`. `to_technology_entities()` exposes the assembled technologies before writing; `build` registers their fuel commodities first. |
| `DemandEntity` | Bundles a demand commodity, its regional-period demand series, and optional time distribution. |
| `FuelSupplyEntities` | Bundles source/fuel commodities, import technologies, distribution technologies, and emission declarations; builds commodities before technologies. |
| `IndustryEntities` | Bundles demands, fuel-serving technologies, and optional free `Other` supply. Assembly is independent of database I/O; `build(db_conn)` writes in dependency order, and `fuel_imports()` derives external requirements. |
| Sector builders | Load evidence, calculate parameters, select entities from validated sector configuration, register datasets, and build the entities in dependency order. |

[`commercial/build.py`][commercial-build] demonstrates the complete chain:
configuration selects enabled end uses, fuels, new technologies, grouping, and profiles;
calculation modules return configured demand/technology entities; the builder writes them
and retains the concrete technologies for fuel declarations.
`existing_technologies.py`, `new_technologies.py`, and `other_end_use.py`
are parameter-block assembly examples, rather than alternate orchestrators.

These objects provide structural inspiration, but direct adoption would affect transport
behavior. Upstream entity writes use insert-or-ignore; local insertion rejects conflicts
or explicitly permits identical rows. Upstream `RowOptions` writes DQ only on the first
parameter row, with source references optional; commercial source registration is still
listed as TODO. `TechnologyEntity` writes rounded fixed lifetimes and currently has no
`LifetimeSurvivalCurve` composition method. Those differences require explicit validation
before using upstream entity writers for transportation.

## New industry evidence and useful local boundaries

The industry module has distinct executable owners rather than one script per parameter
with its own orchestration:

| Upstream owner | Implemented contract | Local use of the example |
| --- | --- | --- |
| [`industry/config.py`][industry-config] | Typed inherited scope plus validated subsector skips/fuel overrides, CEUD year, `Other` treatment, split operator, and error/warning behavior; rejects unsupported or empty fuel lists and an entirely skipped sector. | Translate agreed base fields at the adapter boundary; keep transport source selections and representations in their existing YAML owners. |
| [`common/ceud.py`][ceud] and sector loaders | Shared CEUD block decoder used by agriculture and industry; loaders retain sector dataset names and native fuel-label maps. The decoder returns a `CEUDEnergyUse` dataclass and rejects missing years/rows and unrecognized numeric markers. | Share demonstrated source mechanics while preserving adapter-specific contracts. Transport's normalized CEUD products remain its accepted inputs; no new lake dependency is needed. |
| `energy_use.py`, `demand.py`, `input_splits.py` | Named calculations return documented frames. `align_demand_and_splits` retains region/subsector combinations with demand and usable fuel splits, reporting excluded combinations through configured validation behavior. | Extract coherent calculation stages from long local builders when needed, with explicit keys, units, coverage and audit results. Preserve current transport missing-data rejection and numerical behavior. |
| [`industry/entities.py`][industry-entities] | Pure assembly of demand/technology objects from frames. Only positive split cells receive efficiencies/splits; unused fuels and subsectors do not create pathways. `IndustryEntities` exposes both ordered construction and fuel requirements. | Use actual prepared pathway rows to determine shared endpoints and declarations. Keep a single assembled contribution and local validated insertion. |
| [`industry/build.py`][industry-build] and `validation.py` | Builder validates base scope, loads evidence, computes frames, registers dataset identities and calls `entities.build(db_conn)`; validation delegates period/region checks to `common.validation`. | Keep run coordination and SQLite ownership above parameterization. An adapter validates the initialized database and inserts the existing contribution within its caller/module transaction. |
| Industry tests | Separate configuration, entity and structure tests cover selection, sparse fuel use and build order. | Verify assembly independently from insertion, then test the complete shared-database seam. |

`FuelGrouping.Shared` now has a concrete industry example: one subsector technology
uses several fuel inputs with annual input splits. This demonstrates object composition,
not a transportation renewable mandate or a PHEV replacement. Industry splits are
region/period rows; local PHEV blend splits are already prepared for their own
region/technology/vintage contract.

Several upstream implementation details should remain local to their evidence: industry
can deduct `Other` fuels or supply them freely without price/emissions; its Atlantic
StatCan shares are temporarily hard-coded in `loaders.py`; share repair and rounding
are industry-specific. Upstream builders also load and calculate inside the transaction.
These are useful inspection points, not rules to import into transport. No runtime
benchmark was conducted, so module organization here is evidence of ownership and
testability, not measured performance.

## Transportation's existing callable building block

The existing [`TransportContribution`](../src/build_transport.py) already bundles
schema-model parameter blocks, structural rows, resolved provenance, and family audits.
Its preparation/insertion functions are the common integration seam used by standalone
assembly; no second transport transformation path is needed. Current coverage is:

| Local owner | Assembled contribution |
| --- | --- |
| `build_existing_capacity` | Road, bus, and off-road technology/vintage capacity and reconciliation. |
| `build_demand` | Regional-period service demand and projection evidence. |
| `road_utilization` | Capacity-to-activity and supported flat annual capacity factors. |
| `build_lifetime_parameters` | Configured fixed lifetimes or supported survival-curve rows. |
| `build_efficiencies` | Fuel/service edges and PHEV blend input splits. |
| `build_costs` | Investment and variable costs, with currency and coverage evidence. |
| `ev_chargers` | Charger capacity, efficiency, investment/fixed costs, and audit products; period-indexed utilization remains outside SQLite. |
| `build_transport` | Technology/commodity templates, family composition, provenance registration, and validated insertion. |

For a compatible initialized connection, the executable local seam is
`prepare_transport_contribution(db_conn, bundle=bundle, template_dir=template_dir)`,
then `insert_transport_contribution(db_conn, contribution, conflict="error")`.
Preparation writes no SQLite rows, although family builders can publish their configured
interim, processed, and validation artifacts. It reuses prepared capacity across dependent
families and records `support_audit` for cohort/efficiency/cost/factor alignment; insertion
rechecks that support before its first write, because the contained row lists are mutable.
A frozen contribution dataclass is not itself an immutable or fully validated model.
Insertion returns the touched row batches.
Use their primary keys with `validation.database_bootstrap.validate_database` before
the caller commits. Artifact ownership and validation routes remain in
[`config/paths.yaml`](../config/paths.yaml).

The proposed `canoe_adapter.py` should expose a callable transport module around this
contribution: validate its request, resolve explicit configuration inheritance, check the
shared schema/base rows, prepare the contribution, insert and validate it in the module's
transaction, then return upstream fuel/emission declarations derived from the assembled
rows. An inspectable preparation method should return the same contribution object.
Keep the current family builders and local row/provenance validation as the implementation.
Unlike `IndustryEntities.build`, the local insertion owner explicitly rejects conflicting
rows and preserves full source/DQ chains. The adapter can expose an object around these
existing functions once the request and ownership contracts are resolved; it need not
convert every parameter block into upstream labeled arrays. Add technology-level views
only when an integration consumer needs them.

## Commodity ownership and integration direction

Transport fuel names should follow CANOE-main's shared commodity contract. The integrated
transport module should consume that contract, reuse shared objects, and assemble its own
technology pathways and service/internal commodities. The current
`inputs/0_canoe_template/` remains the standalone baseline's structural input; its combined
commodity list does not establish ownership of every row in a multi-sector build.

Current upstream code implements this ownership in stages, rather than declaring all
commodities in base configuration:

| Object | Current upstream owner | Transport integration behavior to review |
| --- | --- | --- |
| Regions, periods, time slices, model-wide economics | `initializer` / base config | Read and validate initialized values; inherit the agreed scope and reference its rows. |
| Gas and CO2-equivalent commodities | Central `emissions.processing.init` | Reuse their definitions and dataset ownership; omit duplicate transport template definitions. |
| `F_ethos` and `F_<fuel>` supply commodities; import/distribution technologies | Fuel module, after sector compilation | Declare requirements through module output; fuel supply creates these objects later. They need not exist when transport prepares its pathways. |
| Sector delivery endpoints such as `T_gsl`, `T_dsl`, `T_h2` | Sectors register `FuelCommodityEntity` using shared enums/naming; fuel validates and supplies them | Adopt canonical names/definitions, reuse existing agreed rows, and register missing required endpoints before declaring imports. |
| Transport service demands, charger outputs, blend outputs, and reviewed conditioning stages | Transport's assembled pathways | Compose only the objects required by the selected paths, keeping their mappings and assumptions in transport configuration. |
| Vehicle/charger technology parameter blocks | Transport builders | Prepare and insert the same validated blocks used by standalone compilation. |

[`FuelCommodityEntity`][fuel-commodity] derives names through
[`get_fuel_commodity_in_sector`][naming]. Fuel supply's validation requires the consuming
sector endpoints to exist before it runs. Consequently, inheritance means adopting the
shared contract and ownership: some objects already exist and are referenced, while sector
delivery commodities are currently registered by the sector itself. A later centrally
declared commodity registry would change the registration owner, without changing which
fuel contract transport targets.

The adapter should classify structural rows by this ownership before insertion and derive
the necessary endpoint registrations from prepared pathways. This gives a concrete route
away from a single template defining both transport and shared objects, while keeping the
existing parameter calculations and provenance path. Ownership filtering, endpoint
registration, and any adopted naming translations need focused shared-database tests;
hydrogen conditioning and renewable blending remain separate representation decisions.

## Configuration boundary for the adapter

Upstream uses TOML configuration and Pydantic validation.
[`InheritsFromBase`][inheritance] resolves omitted or explicitly inherited fields using
`model_validate(..., context={"base": base})`; same-name fields inherit directly,
while `_INHERIT_RESOLVERS` handles renames such as
`database_file <- base.db_output_dir`. Explicit sector values can override inherited
ones, so compatibility with the seeded database must still be checked. Transport's source,
mapping, representation, and assumption settings remain in its owning YAML files.

The following is an integration mapping to review, not implemented inheritance:

| CANOE-main/base field | Transport boundary |
| --- | --- |
| `db_output_dir` / sector `database_file` | Shared connection target; do not invoke standalone database creation/publication. |
| `future_periods`, `existing_periods`, `period_step` | Validate against `scenario.periods.model`, `existing`, and `step`; exclude the horizon marker from parameter rows and retain the separately configured base year. |
| `provinces` | Reconcile source-region and output-region codes explicitly. Current scenario aggregation selectors normalize BC/BCT aliases for source-role lookup; this does not establish equivalence of the model regions. The BCT/BC geography seam still needs agreement. |
| `model_currency_year`, `global_discount_rate` | Reconcile transport economics, CAD units, and loan rate with base-owned metadata; transport insertion must not overwrite model-wide economics. |
| GDP/price projection settings | Confirm scenario and period-end semantics per parameter family; do not assume the upstream GDP scenario selects all transport evidence. |
| `data_version`, `get_dataset_code()` | Define module identity while retaining transport's source/transformation-derived row dataset IDs and registries. |
| `data_cache_config` | Upstream `GoldConnector` describes a dated silver-layer lake cache. It does not substitute for transport's registered sources, physical validation, provenance, or offline readiness. |

## Implemented fuel coupling and transportation limits

[`declare_fuel_imports`][fuel-declarations] derives requests from actual technology
efficiency inputs and their populated regions. It recognizes only the sector fuel names
produced by `get_fuel_commodity_in_sector`, using the enums in `common/`.
It ignores demand commodities, intermediate commodities, and differently named inputs.
The transport equivalent should derive declarations from prepared efficiency edges,
including blend and charger technologies, rather than listing every template fuel.

[`fuel/build.py`][fuel-build] merges requests by sector/fuel and consuming province.
[`fuel/entities.py`][fuel-entities] builds the chain
`F_ethos -> F_<fuel> -> <sector>_<fuel>`, with import/distribution technologies
between those commodities. `fuel/prices.py` splits delivered prices into an import
cost and a sector distribution cost; the import cost uses the minimum across the fixed
price-source table, including sectors that did not run. Upstream emissions attach to fuel
imports and combustion emissions to sector distribution technologies.

The fuel registry includes transportation gasoline, diesel, CNG, LNG, jet fuel, SPK,
marine diesel, heavy fuel oil, and hydrogen price routes. Electricity is explicitly skipped
by fuel supply for a future electricity module. Successful fuel compilation therefore
does not establish electricity supply for the transport charging chain. The local
`T_gsl`, `T_dsl`, `T_cng`, `T_jtf`, `T_spk`, `T_mdo`,
`T_hfo`, and `T_lng` names match the upstream naming rule, but local blended-fuel
specifications still need price/emission/representation reconciliation.

Renewable components exist: [`fuel/loaders.py`][fuel-loaders] supplies fixed price evidence
for ethanol, renewable diesel, and SPK. However, the reviewed fuel configuration/entities
do not implement mandated mixing proportions or renewable-blend technologies; the supply
entities are transfers with efficiency 1. The renewable-diesel price's biodiesel/HDRD mix
is a price-source assumption, not a final diesel blend mandate. Local blended-fuel labels
therefore cannot be treated as equivalent to upstream component commodities solely because
their names match. Review blend responsibility, proportions, costs, and emissions before
changing their pathways.

The fuel module implements a generic priced hydrogen import/distribution route to `T_h2`,
but its [bundled upstream emission-factor table][upstream-factors] has no hydrogen row.
Hydrogen life-cycle treatment therefore remains unresolved. The reviewed fuel entity path
does not model hydrogen
production facilities, compression, vehicle pressure grades, or dispensing. Transport
should target `T_h2` as the upstream delivery interface; its current `T_h2_ldv` and
`T_h2_mhdv` inputs need either reviewed downstream conditioning paths or an accepted direct
consumption representation. Returning a Hydrogen declaration alone would not connect
those technologies. The supply commodity should retain CANOE-main's name.

`T_elc_ldv_chrg`, `T_elc_mhdv_chrg`, and PHEV blend outputs are also internal
transport commodities rather than direct fuel-supply requests.

## Adapter readiness and unresolved contracts

A full `canoe_adapter.py` remains deferred in this refresh. Industry establishes a useful
object/module lifecycle, but no upstream transportation config, row-ownership agreement or
compatible initialized-database fixture is provided. A wrapper around standalone compilation
would bypass shared assembly; a new entity writer would duplicate validated insertion.
The callable contribution seam is implemented and tested, but a registered upstream
`run()` cannot yet be specified without resolving the following contracts. These are integration tasks, not accepted
changes to baseline transport behavior.

- **Region meaning:** transport `BCT` includes British Columbia and territories;
  upstream [`CANOEProvince`][provinces] seeds `BC` and has no `BCT` member.
  Its NRCan access helper acknowledges the source's BCT aggregation, but does not resolve
  model-region ownership. `NL -> NLLAB` and `PE -> PEI` already have local output
  mappings; the BCT seam requires explicit agreement rather than a string relabel.
- **Schema compatibility:** upstream pins `canoe-schema` to
  `0025a069cbfa31895567b59ef5722a8ab3848eed`; this backend verifies
  `1e68c377d5a7499c78b009d7c472ffd5a6b44901`. The [pinned upstream `Technology` model][upstream-schema]
  also lacks `notes`. Local template preparation requires the documented
  `TransportationTechnology.notes` extension. Upstream initialization calls its pinned
  package DDL and does not apply this local extension. Reconcile the schema contract before
  insertion; preserve notes explicitly.
- **Shared structural rows:** upstream initializes `co2`, `ch4`, `n2o`, and `co2e`
  before sectors. The transport commodity template includes them under its internal dataset;
  `co2e` also differs in units (`kt` locally, `ktCO2e` upstream). Because commodity
  keys include `data_id`, separate datasets can create multiple definitions with the same
  name. Agree ownership/reuse and provenance before passing all local template rows through.
- **Fuel and emissions alignment:** resolve hydrogen endpoints, electricity supply, blend
  specifications, and combustion/upstream emission ownership while adopting the upstream
  fuel commodity contract. Industry returns fuel imports without local emission declarations;
  commercial can write its own input-emission factors and declarations, while fuel supply
  also has combustion-factor routes. Explicitly establish which owner writes each transport
  emission edge and prevent counting combustion twice. Reconcile with legacy parity
  evidence before adding transport emission declarations or accepting upstream fuel defaults.
- **Provenance and registration:** audit dataset/source namespaces, complete DQ preservation,
  conflict handling, backend import/packaging, and the typed transportation sector config.
  Add registration to upstream's sector union only with a working module and integration
  tests; keep source acquisition and cache refresh under their existing transport contracts.

The six local schema and caller-contribution tests were rerun for this refresh. They confirm
template contribution preparation/insertion, caller rollback, incompatible-schema rejection,
and the local pinned schema contract. Additional parameter-support and scenario-comparison
fixtures pass; [the diagnostic](codebase_diagnostic_snapshot.md) records the full check scope.
They do not establish complete multi-sector integration or legacy parity. The next adapter
slice should include an upstream-initialized database fixture, scope/economics checks,
connected fuel endpoints, shared-row/provenance validation, and rollback tests before
a combined compiler run.

## Upstream retrieval map

All upstream links below use the reviewed commit. Re-check the branch head and fetch only
the affected seam when continuing work.

| Seam | Owning files |
| --- | --- |
| Lifecycle and base rows | [`pipeline.py`][pipeline], [`initializer.py`][initializer], [`sector_config.py`][registration]. |
| Module/config/transaction contracts | [`common/module_interface.py`][interface], [`module_inheritance.py`][inheritance], [`db_tools.py`][transactions], [`validation.py`][common-validation]. |
| Reusable entity framework | [`canoe_objects/`][objects]: `technology.py`, `fuel_serving_tech.py`, `demand.py`, `commodity.py`, `parameter.py`, `labeled_array.py`, `array_types.py`. |
| Industry assembly and selections | [`industry/`][industry]: `config.py`, `build.py`, `entities.py`, `energy_use.py`, `demand.py`, `input_splits.py`, `loaders.py`, `subsectors.py`, `validation.py`; [industry structure tests][industry-tests]. |
| Commercial operational example | [`commercial/`][commercial]: `config.py`, `build.py`, `validation.py`, `existing_technologies.py`, `new_technologies.py`, `other_end_use.py`. |
| Fuel supply and declarations | [`fuel/`][fuel]: `config.py`, `build.py`, `entities.py`, `prices.py`, `validation.py`; [`canoe_objects/fuel_imports.py`][fuel-declarations]. |
| Shared names, scope, evidence access | [`common/`][common]: `naming.py`, `fuels.py`, `sectors.py`, `provinces.py`, `periods.py`, `currency.py`, [`ceud.py`][ceud], `cache_connector/`. |
| Emissions and dependency pins | [`emissions/processing.py`][emissions], [`pyproject.toml`][dependencies], `uv.lock`; [upstream pinned v4 row models][upstream-schema]. |

[snapshot]: https://github.com/CANOE-main/CANOE/commit/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b
[pipeline]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/pipeline.py
[initializer]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/initializer.py
[registration]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/sector_config.py
[interface]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/module_interface.py
[inheritance]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/module_inheritance.py
[transactions]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/db_tools.py
[common-validation]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/validation.py
[objects]: https://github.com/CANOE-main/CANOE/tree/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/canoe_objects
[commercial]: https://github.com/CANOE-main/CANOE/tree/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/commercial
[commercial-build]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/commercial/build.py
[fuel]: https://github.com/CANOE-main/CANOE/tree/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/fuel
[fuel-build]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/fuel/build.py
[fuel-entities]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/fuel/entities.py
[fuel-declarations]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/canoe_objects/fuel_imports.py
[fuel-commodity]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/canoe_objects/commodity.py
[naming]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/naming.py
[fuel-loaders]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/fuel/loaders.py
[upstream-factors]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/fuel/data/upstream_emission_factors.csv
[common]: https://github.com/CANOE-main/CANOE/tree/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common
[provinces]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/provinces.py
[emissions]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/emissions/processing.py
[dependencies]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/pyproject.toml
[upstream-schema]: https://github.com/CANOE-main/canoe-schema/blob/0025a069cbfa31895567b59ef5722a8ab3848eed/canoe_schema/v4_0/models.py

[industry]: https://github.com/CANOE-main/CANOE/tree/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/industry
[industry-config]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/industry/config.py
[industry-build]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/industry/build.py
[industry-entities]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/industry/entities.py
[industry-tests]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/tests/test_industry_structure.py
[ceud]: https://github.com/CANOE-main/CANOE/blob/93241c33b1a1ca6fbc1bd855008a580da1ae8e2b/src/canoe/common/ceud.py
