# Scenario configuration

Copy **every section and field** in `legacy_reproduction.yaml` when creating a scenario.
Change selections and output names; keep the same version-3 contract. All implemented
parameter layers run by default. There are no per-layer enable flags.

Source identity, availability, access, native metadata, notes and DQ belong in
`config/sources.yaml`. Each scenario owns its regional fleet aggregation selections
under `aggregation_sources`. Extraction and modelling rules belong in
`config/parameters/rules.yaml`, grouped under `fetching` and `parameterization`.
Reusable conversions belong in `config/parameters/conversion.yaml`.

## Shared run settings

| Section | What it controls |
| --- | --- |
| `scenario` | Run name, description, and purpose. |
| `geography.regions` | Regions covered by the registered inputs and template. Quote `"ON"` to avoid YAML 1.1 boolean parsing. |
| `periods` | Base year, existing vintages, model periods, and step. Existing vintages must end at the base year. Model periods use projections at the period's end: period 2025 with step 5 uses 2030 conditions. |
| `sources.selections` | CEUD provincial/national years, Transport Canada dashboard year, and CER edition. Other sources have pinned registry releases or their own discovery contract; an unused edition/year selector is rejected. |
| `aggregation_sources` | Regional fleet evidence shared across parameters: stock ages, LDV classes, medium-truck classes and heavy-truck haul activity. Every scenario declares the same four roles, with its own selections. |
| `economics.cer_scenario` | CER trajectory used for currency harmonization, independently of demand. It must exist in the selected CER edition. |
| Other `economics` fields | Discount and loan rates, CAD currency, and reference dollar year for costs. |
| `outputs` | Database filename, validation report, and setup log. Artifact directories remain owned by `config/paths.yaml`. |
| `comparison` | Optional diagnostics: `mode: none`, `legacy`, or `scenario`, reference SQLite path, numerical tolerances, and provenance inclusion. Integrity validation always runs. |
| `row_note_overrides.technology` | Replace `technology.notes` by exact technology key for this scenario. Keep `{}` to retain template notes; unknown keys fail. This does not change parameter values or source notes. |

All registry sources marked `active` are available to implemented ETL layers. Scenarios
do not maintain a second active-source list. A source year selects registered evidence;
it does not create missing caches, mappings, or template periods.

## Parameter settings

| Section and field | Selections and behavior |
| --- | --- |
| `lifetimes.survival_curves` | `true` uses accepted road survival curves; `false` uses configured fixed lifetimes, including source-derived medians for LDVs/medium trucks and the manual heavy-truck lifetime. Other technologies retain their configured fixed lifetime pathway. |
| `lifetimes.survival_curve_max_age` | Maximum supported age for road survival curves and existing-stock cohorts when curves are enabled. Fixed-lifetime cohorts use their class lifetime. |
| `existing_capacity.vehicle_population_year` | MTO age-evidence year for LDVs, trucks, motorcycles, and buses. It must match the reviewed LDV aggregation artifact and the available Report 5 edition. |
| `existing_capacity.cleanup_tolerance` | Retain positive road/bus/off-road capacity at or above this threshold in each row's native capacity unit. This is a capacity cutoff, independent of comparison tolerances; it does not trim costs or round parameters. Charger capacities have their own preparation. |
| `demand.cer_scenario` | CER demand trajectory, also used for the normalized default-source marker. Must exist in the selected CER edition. |
| `demand.future_car_demand` | `GDP-indexed` or `extrapolated`. Extrapolation uses configured historical car CAGR; light trucks receive the remainder of the unchanged GDP-indexed combined passenger demand. History and harmonization remain in rules. |
| `road_utilization.vkt_schedules` | `false` uses flat CEUD utilization. `true` prepares age-dependent mileage artifacts for supported classes. |
| `road_utilization.vkt_max_age` | Mileage-profile age limit, independent of the survival-curve age limit. Must cover a full model period when schedules are enabled. |
| `efficiencies.atb_trajectory` | `Advanced`, `Conservative`, `Constant`, or `Mid`; selected from the pinned ATB release. |
| `costs.atb_trajectory` | The same ATB choices, independently selectable from efficiencies. |
| `ev_chargers.ld_evs_per_port` | Positive LDV electric vehicles per charger port. |
| `ev_chargers.mhd_evs_per_port` | Positive medium/heavy-duty electric vehicles per charger port. |

The current v4 capacity-factor table lacks the joint vintage/period grain needed for
age-dependent road utilization. Enabling VKT schedules retains those rows as audit
artifacts; only supported flat classes enter that table. Charger utilization likewise
remains an artifact while its period dimension is unsupported. These are schema limits,
not additional scenario switches.

Unannotated source/component DQ indicators resolve to **5** from the source registry's
defaults. Explicit component scores override source scores one indicator at a time.
Blank placeholders do not prevent scenario builds. Source `database_note` fields remain
empty until annotated; component notes may override them.

## Shared regional aggregation

Each scenario owns `aggregation_sources`, with explicit `"ON"` and `other` selections
for every fleet role. Add a region key to override `other`; `BC` covers `BCT` unless
`BCT` is explicit. A new source must be registered and active in `sources.yaml`, with
an implemented aggregation adapter. These selections apply consistently across parameters and future embodied
emissions; there are no parameter-specific aggregation-source selectors.

| Role | Current regional treatment |
| --- | --- |
| `stock_age` | MTO Report A LDV age/vintage shares and Report 5 truck, bus, and motorcycle shares in every region. Provincial stock totals retain their CEUD/registration evidence. |
| `ldv` | MTO Report A class weights everywhere, for ratings, ATB efficiency/cost, utilization and NHTSA lifetime products. Wards LDV survival products remain opt-in comparison evidence. |
| `medium_trucks` | ON uses Report 4 including Class 2/2b; other regions use Wards Classes 3–7. Class 2b maps to Class 2 ATB VMT. All available freight vocations receive equal weights within GVWR class, excluding School/Transit/Refuse; identical VMT schedules retain separate vocation weights. |
| `heavy_truck_haul` | StatCan Table 23-10-0142-01 provincial origin-or-destination tonne-km shares, pooled over 2011–2017 with within-province shipments counted once, shared by efficiency, costs and utilization. The configured geography map currently uses BC alone for BCT. |

Medium-truck lifetime products use the single NHTSA CAFE 2b/3 Trucks survival curve for
every GVWR class in every region. Aggregating identical curves with Report 4 or Wards
weights leaves their lifetime values unchanged. Heavy trucks use the fixed lifetime in
`inputs/0_manual_params/lifetime_process.csv` when survival curves are off, and the
NEMS Classes 7–8 scrappage-derived curve when they are on. Native class maps, weight basis,
Wards year and vocation exclusions belong in `rules.yaml` under `road_aggregation`.
The population year remains a scenario selection and must match the Report A/4/5
editions consumed by that scenario.

## Cleanup and cross-table support

The capacity builder removes zero and sub-threshold stock with a per-key audit and
totals by unit. Efficiency and cost builders then select retained existing
`(region, tech, vintage)` keys. The assembled contribution checks those relationships
again after charger rows have been added and immediately before caller-owned insertion:

| Legacy cleanup purpose | Current guarantee |
| --- | --- |
| Remove negligible capacity and dependent historical rows | Capacity cutoff and retained-key generation, followed by historical efficiency/cost support checks. Historical vintages come from the scenario, replacing the legacy hard-coded 2021 boundary. |
| Remove costs without efficiency | All prepared investment, fixed, and variable cost keys must have efficiency at the same region, technology, and vintage, including future investments and chargers. |
| Remove unsupported new-technology annual factors | New technology factors must have efficiency at their exact v4 vintage. The old period-based SQL is not applicable to v4's vintage-based factor table or age-dependent audit artifacts. |
| Remove technology-only scaling/lifetime entries without efficiency | Retained as defaults: these entries do not activate a technology/vintage. Inactive existing technology and not-yet-parameterized template defaults are useful. The legacy function's final loop also reused tech/vintage tuples as technology IDs and did not implement its stated intent correctly. |

Unexpected assembled support gaps fail before database writes rather than silently
discarding generated costs. `parameter_support` in the build report records checked
key counts; `existing_capacity.cleanup` records actual removals. Development callers
that explicitly omit a layer receive an audit limited to prepared dependency tables.
The legacy emission-activity cleanup has no current parameter batch to act on; when
that layer is implemented, its support contract must be added at this same boundary.

## SQLite comparisons

Use `comparison.mode: legacy` for the registered old baseline. Its adapters compare
Ontario transport rows and reconcile selected old table names and unit labels. It is
a parity diagnostic, and differences are not automatically accepted tolerances.

For a second current-backend scenario, set `comparison.mode: scenario` and point
`comparison.reference_sqlite` to its existing database (for example,
`outputs/sqlite/canoe_transport_other_scenario.sqlite`). This compares all tables and
regions on exact primary keys, reports missing keys and changed values, and compares
units/text exactly. Numerical values use `absolute_tolerance` and `relative_tolerance`.
Unkeyed solver output tables use exact row multisets, preserving duplicate counts.
Schema mismatches or duplicate model keys after excluding provenance are reported as
non-comparable. This is a database difference report, not a solver-feasibility check.

`include_provenance: false` excludes source registry tables, source IDs, dataset IDs,
DQ scores, and notes. Set it to `true` to compare those too in scenario mode. Legacy
mode requires `false` because old provenance is not the v4 contract. References are
opened read-only; an enabled comparison needs an existing reference different from the
output database. `mode: none` disables only comparison; keep every template field,
using `reference_sqlite: null` if unused. Results live under `comparison` in the
configured validation report; integrity/provenance/FK checks live under `validation`.

## Validation and execution

Configuration loading rejects unknown fields, missing parameter controls, duplicate YAML
keys, inactive/unsupported source selectors, invalid period grids, unregistered ATB/CER
choices, invalid DQ scores, and rates outside zero to one. Scenario paths are accepted by
the existing Python entrypoints and Snakemake workflow.

Snakemake uses cached inputs without downloading by default. Pass
`--config download_sources=true` to refresh sources; this is an execution option.

Version 3 replaces `switches` with `lifetimes` and `road_utilization`, adds separate
efficiency/cost ATB selections and an economic CER selection, and moves the MTO
age-evidence year and existing-vintage grid out of rules. Existing scenarios should be
migrated by copying the full template and transferring their selections; obsolete fields
are rejected so they cannot silently stop affecting the run.
The comparison controls replace `validation.compare_legacy`, and the charger and
cleanup fields use the clearer names in the template. Former population-source and
medium-truck aggregation selectors are replaced by the shared regional source policy.
