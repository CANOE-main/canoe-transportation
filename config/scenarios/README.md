# Scenario configuration

Copy **every section and field** in `default.yaml` when creating a scenario.
Change selections and output names; keep the same version-3 contract. All implemented
parameter layers run by default. There are no per-layer enable flags.

Source identity, availability, access, native metadata, notes and DQ belong in
`config/sources.yaml`. Each scenario owns its regional fleet aggregation selections
under `aggregation_sources`. Extraction and modelling rules belong in
`config/parameters/rules.yaml`, grouped under `fetching` and `parameterization`.
Reusable conversions belong in `config/parameters/conversion.yaml`.

## Supported roles and lifecycle

Keep one YAML per demonstrated, repeatable workflow. The current supported set is:

| File | Role and current scope |
| --- | --- |
| `default.yaml` | Canonical production entrypoint/template: ten provinces, prospective 2025, sales-derived calibrated cohorts, MOVES fixed lifetimes and new-capacity range shares. Its run label remains `baseline`. |
| `legacy_comparison.yaml` | Reproducible legacy-timing comparison: ten provinces, observed 2023, NHTSA/NEMS survival-curve selection and configured legacy SQLite comparison. Uses the shared constant-retirement tail to 0.5% survival, then zero. |
| `validation_baseline.yaml` | Ontario prospective-2025 comparison for both legacy variants: 2023 CEUD held constant for historical parameters, sales-survival stock anchored to CEUD, MOVES fixed lifetimes and representative new LDV ranges. |
| `validation_energy_calibration.yaml` | Matched Ontario calibration-on comparison: same fleet/activity/source/representation as validation_baseline, optional CEUD reported-liquid-energy adjustment enabled and its diagnostic evidence captured. Scientific acceptance remains separate. |
| `notebook_diagnostics.yaml` | Shared stock/efficiency notebook prerequisite: Ontario observed-2023, NHTSA/NEMS curves, explicit future ranges, both captured diagnostics, no charging profile or embodied emissions. |

Retain these names and selections. For a new supported comparison, use a concise
`validation_<question>.yaml` name; add another diagnostic
preset only when the shared notebook preset cannot serve a demonstrated consumer.
Use lowercase snake_case for filenames and run labels. The filename identifies a
configuration; `scenario.name` identifies its report/output family and need not match
an existing filename. Record purpose, consumer and changed selections in the scenario.

Keep temporary alternatives as complete YAMLs under ignored
`tmp/<task>/scenarios/exp_<question>_<variant>.yaml`, outside the supported set. Start
from the appropriate complete control/template and change only the intended selections;
there is no scenario inheritance or override manager. Give every variant distinct
database/report/setup-log names through the existing output fields. Source years,
modeling selections and comparisons still belong in that YAML. Build from the repository
root using `--scenario <path>`.

Distinct final outputs do not isolate shared preparation artifacts: run native scenarios
serially. Tests or concurrent experiments need a disposable repository/configuration
with all mutable artifact routes isolated, not only a different SQLite filename.
Remove task scratch after its useful results and configuration identities have been
recorded in their existing report or task owner. Promote a temporary scenario only for
a recurring supported workflow, with a documented consumer and appropriate validation.
On retirement, repair active consumers and retain captured configuration/hash identities;
do not recreate deleted presets to satisfy historical prose or reports.

Retired researcher-scenario databases and captures are removed when no live consumer
uses them. The transportation-validation notebook uses
`transportation_research.notebook_scenario` in `config/parameters/rules.yaml`, currently
`validation_baseline.yaml`. Both fleet and charger captures must match that live YAML
and the same fresh database/build fingerprints. Regenerate both after changing it.

Generated databases under `outputs/sqlite/`, run reports, comparison captures and
notebook HTML remain local and are ignored by Git. Keep their versioned scenarios,
producers and registered inputs so the supported commands can rebuild them. Unique
review handoffs and authored presentation files are retained separately; do not use
blanket generated-output deletion to clean them.

## Shared run settings

| Section | What it controls |
| --- | --- |
| `scenario` | Run name, description, and purpose. |
| `geography.regions` | Regions covered by the registered inputs and template. Quote `"ON"` to avoid YAML 1.1 boolean parsing. |
| `periods` | Period mode, parameter base year, existing vintage labels, model periods, and step. Prospective periods use end conditions; legacy retains the previous historical bins and future timing. Source observations retain their own dates. |
| `sources.selections` | CEUD provincial/national years, Transport Canada dashboard year, and CER edition. Other sources have pinned registry releases or their own discovery contract; an unused edition/year selector is rejected. |
| `sources.nrcan_ceud_beyond_observed` | `error`, `constant`, or `extrapolated` for stock and historical parameter views through base_year. Extrapolated uses `ceud_inputs.cagr` controls, omitting intervals touching 2020–2021. Demand always anchors at the actual selected-edition observation and uses CER real GDP to base_year and future projection years; the optional passenger split shares the CAGR estimator, not this endpoint policy. |
| `aggregation_sources` | Regional fleet evidence shared across parameters: stock ages, LDV classes, medium-truck classes and heavy-truck haul activity. Every scenario declares the same four roles, with its own selections. |
| `economics.cer_scenario` | CER trajectory used for currency harmonization, independently of demand. It must exist in the selected CER edition. |
| Other `economics` fields | Discount and loan rates, CAD currency, and reference dollar year for costs. |
| `outputs` | Database filename, validation report, and setup log. Artifact directories and the automatic build-log naming template remain owned by `config/paths.yaml`. |
| `comparison` | Optional diagnostics: `mode: none`, `legacy`, or `scenario`, reference SQLite path, numerical tolerances, and provenance inclusion. Integrity validation always runs. |
| `row_note_overrides.technology` | Replace `technology.notes` by exact technology key for this scenario. Keep `{}` to retain template notes; unknown keys fail. This does not change parameter values or source notes. |
| `notebook_diagnostics` | Optional list containing `vehicle_stock`, `efficiency` and/or `ceud_calibration`; omitted means no notebook preparation. The stock/efficiency diagnostic preset selects the first two. The calibrated validation preset selects the third, which requires energy calibration enabled. These captured review artifacts never parameterize production rows. |

All registry sources marked `active` are available to implemented ETL layers. Scenarios
do not maintain a second active-source list. A source year selects registered evidence;
it does not create missing caches, mappings, or template periods.

Every scenario build automatically writes timestamped INFO, WARNING and ERROR
messages, stage timings and failure tracebacks to its log. The naming template
`outputs.build_log` in `config/paths.yaml` combines `{scenario_name}` (the run label)
and `{scenario_yaml}` (the YAML filename without its extension), followed by
`{timestamp}` (UTC run start, `YYYY-MM-DD_HH-mm-ss-microsecondsZ`); for example,
`outputs/logs/baseline_default_2026-10-22_17-30-00-123456Z.log`.
Each run creates a separate file. Filename-unsafe characters become underscores.
Each attempt records the exact YAML path and SHA-256, with start and outcome markers.
`--debug` includes detailed audits; the validation report records the resolved log
path and verbosity. Scenario YAMLs need no separate build-log filename.

## Road diagnostic preset

Prepare both notebooks with one offline native build:

```text
uv run python src/build_transport.py --scenario config/scenarios/notebook_diagnostics.yaml --overwrite
```

The preset compiles an Ontario database with explicit future PEV ranges and prepares
complete registered MTO history, reviewed-mapping diagnostics, retention evidence,
both heavy-truck survival sources, and the opt-in MTO rating-weighting benchmark.
It does not regenerate the reviewed map. Add `--download-sources` only to permit
acquiring missing registered caches.

After a successful build, `outputs/validation/notebook_evidence/latest.json` points
to local bundles containing losslessly compressed CSVs and the configuration used by each
notebook, file hashes, build time and database identity. Open either notebook and
reload after a new preset build. There is no notebook scenario selector. Later
ordinary builds leave these bundles intact; they continue to describe the recorded
diagnostic build until the next explicit preparation. Editing a live YAML file
cannot reinterpret an older bundle. Detailed parameter choices remain in the preset.

Only evidence regenerated by supported ETL or notebook commands belongs to this
handoff. The optional MTO benchmark is calculated by its versioned diagnostic routine
and remains labeled as a benchmark. Agent judgments, task acceptance comparisons,
browser captures and probe logs are temporary development work under ignored `tmp/`;
remove them when the task ends. They are not source inputs or reproducibility proof.
Notebook bundles are ignored local outputs. After a successful publication the builder
removes superseded bundles; a failed build preserves the latest successful evidence.
Keep registered caches, external models, reviewed mappings and notebook source files.

## Parameter settings

Build logs report elapsed and process CPU time for prerequisites, parameters,
insertion and final checks.
Efficiency preparation also reports family-matching and regional composition times.
Detailed count-weight audits are available with `--debug`. Preparations reuse
matching/cohort results, physical costs and numerical source points within their
current inputs. Every build replays registered inputs and performs its configured
validation. Numerical sensitivity callers clearing the ATB value cache read the
current ATB values. CEUD/load evidence is fixed within one preparation instance.

| Section and field | Selections and behavior |
| --- | --- |
| `lifetimes.survival_curves` | `false` exports calibrated physical survivors with fixed lifetimes. `true` exports installed-equivalent acquisitions with annual vintage survival and explicit process lifetimes; it uses the approved shared tail rule and validates annual curves and lifetimes. Buses/motorcycles retain their existing pathway. |
| `lifetimes.road_lifetime_survival_source` | `nhtsa_nems` selects NHTSA car/light/medium and NEMS heavy schedules. `moves` selects MOVES car/light/heavy schedules, with medium trucks using heavy rates. One choice supplies medians, historical stock survival and future curves. |
| `lifetimes.survival_curve_max_age` | `null` derives each end age using the selected survival threshold, then zero. A positive integer shortens any longer derived curve by forcing zero at that age and audits the removed stock. |
| `lifetimes.tail_survival_threshold` | Surviving fraction that ends the derived tail; default `0.005` means 0.5%. The final positive year is followed by zero. |
| `lifetimes.tail_derivation` | Only `constant_retirement` is supported: keep native survival, extend the final annual retirement rate to the selected threshold or less, retain that positive year, then append zero. Select the surviving fraction with `tail_survival_threshold`. |
| `existing_capacity.vehicle_population_year` | MTO age-evidence year for LDVs, trucks, motorcycles, and buses. It must match the reviewed LDV aggregation artifact and the available Report 5 edition. |
| `existing_capacity.cleanup_tolerance` | Retain positive road/bus/off-road capacity at or above this threshold in each row's native capacity unit. This is a capacity cutoff, independent of comparison tolerances; it does not trim costs or round parameters. Charger capacities have their own preparation. |
| `demand.cer_scenario` | CER demand trajectory, also used for the normalized default-source marker. Must exist in the selected CER edition. |
| `demand.future_car_demand` | `GDP-indexed` or `extrapolated`. Extrapolation uses configured historical car CAGR; light trucks receive the remainder of the unchanged GDP-indexed combined passenger demand. History and harmonization remain in rules. |
| `road_utilization.vkt_schedules` | `false` uses flat CEUD utilization. `true` prepares age-dependent mileage artifacts for supported classes. |
| Road mileage age | Derived from the explicit survival age cap or the longest derived class endpoint. Omit `road_utilization.vkt_max_age` from YAML; the retired independent setting is rejected. Missing ATB age coverage fails explicitly. |
| `efficiencies.atb_trajectory` | `Advanced`, `Conservative`, `Constant`, or `Mid`; selected from the pinned ATB release. |
| `efficiencies.ceud_energy_calibration` | Required boolean. `false` preserves uncalibrated efficiency. `true` fits supported CEUD gasoline/diesel class totals on matched physical stock/activity, retaining unadjusted evidence and unsupported coverage. Requires fixed road lifetimes and flat utilization. The effective class multiplier transfers to all powertrains/years; electricity is not independently calibrated. See the [energy method](../../docs/model/energy.md#optional-ceud-energy-calibration). |
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
artifacts; only supported flat classes enter that table. Charger utilization uses
supported vintage-indexed rows and does not require that extra period dimension.

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
`uv run python -m fetching.greet_automation --scenario config/scenarios/default.yaml`.
The runner uses disposable copies of the configured pair and publishes gas,
exclusion, case and report evidence with a manifest. Ordinary preparation and the
native scenario build validate and read this bank offline, without starting Excel. After
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

`periods.base_year` is the parameter anchor used for stocks, demand,
ratings, load factors and annual turnover. CEUD extension to this anchor requires
an explicit `sources.nrcan_ceud_beyond_observed` selection of `constant` or
`extrapolated`. Extended values are labeled estimates. It is independent of
`economics.cost_reference_year`, which expresses costs in a common dollar basis.
With the template, observations end in 2023 and costs are expressed in 2020 CAD.
Changing the dollar year does not change the observation year or period labels.

### Sales reconstruction and later CEUD base years

Cars and trucks use one production sales-derived method. The removed
`existing_capacity.road_stock_method` selector is rejected; stock-age allocation
remains available only in the stock/cohort diagnosis. Buses and motorcycles are unchanged.
A later base year needs a matching period grid and an explicit constant or extrapolated
CEUD extension. Held values are estimates, never new observations.

For each region/class, compute physical survivors as annual sales times survival to
`periods.base_year`, then multiply acquisitions and survivors by
`k = CEUD base-year stock / raw surviving stock`. Current sales begin in 2000, so
calibration absorbs missing older cohorts and other source mismatches without resolving
them. Keep the annual powertrain allocation rules and their explicit overrides.

Curves off exports calibrated survivors after fixed-lifetime redistribution. Curves on
exports calibrated installed-equivalent acquisitions with acquisition-weighted annual
vintage curves and matching process lifetimes. The latter retains the summed annual
survival path until the explicit small-tail retirement rule; it does not match future CEUD stocks. Apply the
same constant-retirement schedule before calibration and vintage grouping. Raw source
survival remains audit evidence. Preserve annual survivor totals when grouping vintages; only values too small for
Temoa end a grouped curve early. Report that adjustment without recalibrating it away. The production
method is assessed in `docs/insights/stock_cohort_diagnosis.py`.

Utilization is reference-year activity divided by calibrated physical surviving stock
times C2A. Existing efficiency aggregation and EV charger counts also use survivors.
Acquisition weights construct the combined vintage survival curves. Calibration closes stock and
activity by construction; energy remains an independent check. Aggregate powertrain
composition can change when the cohort ages change, even with unchanged annual shares.

`parameterization.ceud_inputs` writes labeled parameter evidence separately from
native normalized files. It extends missing tails of currently reported series
through the base year and preserves all native observations. `constant` holds the
last reported value. `extrapolated` applies historical CAGR from eligible observed
annual changes under `ceud_inputs.cagr`, excluding intervals touching 2020–2021.
The same estimator supports `demand.future_car_demand: extrapolated`. Generated
values never train this rate. Zero endpoints remain zero, and insufficient positive
history or invalid projections fail explicitly. Road efficiency projections use the
separate aligned NRCan/ATB 2023 bridge described in `docs/assumptions.md`.

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
  survival_curve_max_age: null
  tail_derivation: constant_retirement
  tail_survival_threshold: 0.005  # Surviving fraction: 0.5%, then zero
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
| `stock_age` | MTO age evidence for the deprecated car/truck diagnostic comparison and unchanged bus/motorcycle allocation. Production cars/trucks use sales-derived cohorts. |
| `ldv` | MTO Report A class weights everywhere, for ratings, ATB efficiency/cost, utilization and NHTSA lifetime products. Wards LDV survival products remain opt-in comparison evidence. |
| `medium_trucks` | ON uses Report 4 including Class 2/2b; other regions use Wards Classes 3–7. Class 2b maps to Class 2 ATB VMT. All available freight vocations receive equal weights within GVWR class, excluding School/Transit/Refuse; identical VMT schedules retain separate vocation weights. |
| `heavy_truck_haul` | StatCan Table 23-10-0142-01 provincial origin-or-destination tonne-km shares, pooled over 2011–2017 with within-province shipments counted once, shared by efficiency, costs and utilization. The configured geography map currently uses BC alone for BCT. |

The `nhtsa_nems` profile applies the NHTSA CAFE 2b/3 schedule to medium trucks
and NEMS Class 7–8 to heavy trucks, with NHTSA car and aggregated light-truck
schedules. The `moves` profile uses native car/light-truck schedules and shares
the heavy-vehicle schedule across medium and heavy trucks. All conditional rates
become cumulative survival with an explicit age-zero baseline. One selected
profile governs stock survival, fixed medians, curves and provenance.

The normal build prepares the registered MOVES6 release when selected. To prepare its optional
notebook comparison evidence independently, run
`uv run python -m fetching.assorted_sources --scenario <scenario.yaml> --moves-survival`.
Add `--no-download` to require the registered PDF cache. Later NEMS builds preserve
this comparison table and its source audit without requiring the unselected PDF.
The earlier `--moves5-survival` flag remains a compatible alias for this registered
source operation. The selected release is owned by `sources.yaml` and extraction rules.

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
the existing Python entrypoints. Build a complete scenario with
`uv run python src/build_transport.py --scenario <scenario.yaml> --overwrite`.
The command replays production prerequisites from registered caches on every invocation,
offline by default. `--download-sources` permits acquisition of missing caches; it does not
refresh existing caches. Paths and all model/source selections stay in YAML. Run scenarios
serially while derived artifact paths are shared. Manual review, optional diagnostics and
external-model generation remain explicit operations.

Version 3 requires the explicit period mode, replaces `switches` with `lifetimes` and `road_utilization`, adds separate
efficiency/cost ATB selections and an economic CER selection, and moves the MTO
age-evidence year and existing-vintage grid out of rules. Existing scenarios should be
migrated by copying the full template and transferring their selections; obsolete fields
are rejected so they cannot silently stop affecting the run.
The comparison controls replace `validation.compare_legacy`, and the charger and
cleanup fields use the clearer names in the template. Former population-source and
medium-truck aggregation selectors are replaced by the shared regional source policy.
