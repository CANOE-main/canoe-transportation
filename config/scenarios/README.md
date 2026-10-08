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
| `periods` | Period mode, observed-data base year, existing vintage labels, model periods, and step. Prospective periods use end conditions; legacy retains the previous historical bins and future timing. |
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
| `lifetimes.survival_curve_max_age` | Maximum supported age for road survival curves and existing-stock cohorts when curves are enabled. The prospective template uses 29 to cover ages 25–29 for vintage 2000 in the first model period; every required source age must exist. Fixed-lifetime cohorts use their class lifetime. |
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
| `charging_profiles.travel_behavior_source` | `none`, `nhts`, or `tts` household travel-survey source for the frozen Ontario simulation. The selected shape applies unchanged to shared LDV chargers in every configured region. |
| `charging_profiles.time_mapping` | Null uses elapsed-hour D001-D365/H01-H24 labels after Toronto-calendar hourly means. A complete `hour_index,season,tod` CSV projects the 8760 physical hours onto inherited CANOE slices; slice durations must agree with season fractions and TOD hours. This selects temporal aggregation/labels, not a time zone. Python callers can supply the same mapping as a DataFrame with their season/TOD rows. |
| `BEV_PHEV_range_representation.mode` | `none` preserves new variants and adds no range rules. `new_capacity_shares` prepares minimum (`ge`) shares of new class/powertrain capacity from the registered OMEGA baseline sales/CD-range evidence. `representative_archetype` replaces new LDV range variants with one BEV/PHEV per category and emits no range constraints. Existing BEVs/PHEVs always use representatives with OMEGA weights, independently of this selection. |
| `embodied_emissions` | Enables the road vehicle-cycle layer. When false, preparation requires no embodied GREET workbooks or result bank and inserts no embodied rows. |
| `embodied_materials` | `conventional` or `lightweight`, for LDVs only. MHDV values and provenance are independent of this selection. The registered release's glider selector mismatch is accepted because the affected saved range inputs are identical; unequal inputs require a reviewed correction before lightweight use. |

The current v4 capacity-factor table lacks the joint vintage/period grain needed for
age-dependent road utilization. Enabling VKT schedules retains those rows as audit
artifacts; only supported flat classes enter that table. Charger utilization likewise
remains an artifact while its period dimension is unsupported. These are schema limits,
not additional scenario switches.

The frozen charging shapes already contain inherited fleet/range/charger composition.
They are peak-normalized hourly power shapes; no new market shares are multiplied into
them. Elapsed-hour labels preserve all 8760 physical hours, including Toronto DST, and
do not identify summer wall-clock hours. The selected Ontario/shared-charger proxies
apply the BEV shape to LDV BEV/PHEV/motorcycle charging in all regions. TTS remains a
weekday-only simulation. Hourly factors are not directly comparable with the legacy
clustered/rescaled profiles. Select `charging_profiles.travel_behavior_source: none` to retain the
previous baseline without this slice's factors.

Range shares describe US regulatory car/light-truck OMEGA baseline sales within each BEV/PHEV
group; they are not adoption shares. Passenger and freight light trucks use the same
light-truck distribution. The accepted MY2022 legacy snapshot proxies the Canadian market
and applies unchanged across model vintages. The evidence command is independent of mode:
`uv run --offline python -m parameterization.ldv_ev_ranges --evidence-only`.
It publishes source-row records, full-denominator buckets and unresolved sales, including
zero-sales buckets. It never substitutes catalogue counts or renormalizes unknown sales.

The active `epa_omega_baseline` registration uses the 78-row, 14 KB extract of the exact
OMEGA file read by `LDV_AER_market_shares.ipynb`. Verify its identity or recreate a missing
extract with `uv run --offline python -m fetching.epa_omega_baseline --verify-legacy-identity`.
Routine ETL requires only the compact file, not the notebook, parent or large model archive.
EPA Trends and FuelEconomy join artifacts remain historical diagnostics; they are not
dependencies or fallback weights for these modes.

Representative preparation accepts the already-prepared parameter batches through
`parameterization.ldv_ev_ranges.prepare_representative_parameters`; it does not acquire
sources or rebuild prerequisites. It conserves sales-weighted consumption (harmonic
efficiency), costs per equal capacity/service unit, surviving cohorts and embodied gases.
C2A, utilization, fixed lifetimes and range survival curves must agree within a family;
unsupported coverage, units or relationships fail explicitly. Each category has one PHEV
blend with the accepted arithmetic mean of consumption-weighted vintage electricity
fractions, applied across periods and vehicle vintages. Total energy remains specific to
the vehicle vintage; gasoline/electricity deviations appear in `representative_conservation.json`.
Historical stock, MHDVs, motorcycles and shared charging shapes retain their existing rows.

Declare both embodied fields even when the layer is disabled. Generate the complete
registered source bank explicitly on Windows with desktop Excel:
`uv run python -m fetching.greet_automation --scenario config/scenarios/legacy_reproduction.yaml`.
The runner uses disposable copies of the configured pair and publishes gas,
exclusion, case and report evidence with a manifest. Ordinary preparation and the
scenario DAG validate and read this bank offline, without starting Excel. After
changing source inputs, controls, anchors or ATB archetypes, regenerate it.

The source adapter selects one simulation target and the expected imported LDV
cohort year; changing these requires fresh validated evidence. The harmonization
rules currently hold the 2025 factors constant through 2050. Factors are lifetime
totals per manufactured vehicle, with no lifetime, mileage, service-output or
annual-rate division. Current vintage coordinates follow `ScenarioPeriods`.

Unannotated source/component DQ indicators resolve to **5** from the source registry's
defaults. Explicit component scores override source scores one indicator at a time.
Blank placeholders do not prevent scenario builds. Source `database_note` fields remain
empty until annotated; component notes may override them.

## Period modes and observed years

`periods.base_year` is the latest observed year used to calibrate CEUD stocks, demand,
ratings, load factors and annual turnover. It is independent of
`economics.cost_reference_year`, which expresses costs in a common dollar basis.
With the template, observations end in 2023 and costs are expressed in 2020 CAD.
Changing the dollar year does not change the observation year or period labels.

`periods.period_mode: prospective` is the template default. A period begins on
December 31 of its label year and ends at the next model label: 2025 covers
2026–2030, and all year-varying future inputs use 2030 conditions. The final label,
2045, uses 2050 inputs; 2050 is added to SQLite only as the horizon marker.
Source values that do not vary by year retain their source evidence and dates.

Historical labels follow the same interval boundaries. Vintage 2015 represents
2016–2020 cohorts; vintage 2020 represents 2021–2025 cohorts, using only observed
2021–2023 data in this run. Historical efficiencies average those available years;
year-varying historical cost evidence uses the last observed year in its bin, with
the existing source-availability proxies and manual baselines. The oldest label
also receives the initial-stock/oldest-cohort proxy. Annual audit rows retain their
source/cohort years separately from their model vintage labels.

The observation year need not appear in `periods.existing`. The default grid ends
at 2020 because 2023 observations already belong there. An explicit 2023 label in
prospective mode has no observed years when `base_year` is 2023; setup and build
reports identify this empty label. Historical efficiency and cost rows follow
retained capacity keys. Lifetime and scaling defaults may still describe an
explicitly declared empty label. Existing charger capacity and the medium-truck
BEV override use the latest bin containing observations: vintage 2020 in the
template. Irregular existing labels are supported; each next label closes the
preceding interval.

For prior-backend timing, use `period_mode: legacy` and include the observation
year as the last existing label. To reproduce the pre-change temporal selections:

```yaml
periods:
  period_mode: legacy
  base_year: 2023
  existing: [2000, 2005, 2010, 2015, 2020, 2023]
  model: [2025, 2030, 2035, 2040, 2045]
  step: 5
lifetimes:
  survival_curves: true
  survival_curve_max_age: 25
```

Legacy cohorts go to the next existing label: 2016–2020 → 2020 and 2021–2023 →
2023. Future efficiency and GDP already used period ends, while costs and charger
schedules used period labels; legacy preserves this mixed timing. The name
`legacy` makes that distinction explicit. Both modes retain the same stock
calibration, source selections, native evidence dates and dollar-year conversions.
The `period_mapping` reports list historical years, empty bins and future read years.

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

Version 3 requires the explicit period mode, replaces `switches` with `lifetimes` and `road_utilization`, adds separate
efficiency/cost ATB selections and an economic CER selection, and moves the MTO
age-evidence year and existing-vintage grid out of rules. Existing scenarios should be
migrated by copying the full template and transferring their selections; obsolete fields
are rejected so they cannot silently stop affecting the run.
The comparison controls replace `validation.compare_legacy`, and the charger and
cleanup fields use the clearer names in the template. Former population-source and
medium-truck aggregation selectors are replaced by the shared regional source policy.
