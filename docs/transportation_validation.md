---
title: Transportation validation and scenario controls
role: Production scenario methods and independent legacy parameter comparisons.
status: implementation_and_evaluation
retrieve_when: Reviewing provincial sales-survival scenarios or legacy parameter comparisons.
verify: Current executable configuration and generated reports govern numerical findings.
---

# Transportation validation

The backend uses registered production evidence and the current canonical vehicle
reference tables. The files under `legacy_backend/validation_1` and `validation_2`
are read-only comparison evidence. Their values never enter source registration,
production calculations or database insertion.

`validation_1/canoe_ON_16d.sqlite` is the supplied Ontario extract of the notebook-derived
all-region model. The comparison prioritizes existing capacity, annual utilization
and demand. The notebook's raw sales bins differ from the backend's annual surviving
cohorts. Its saved fixed-lifetime output conflicts with its current switch, so no
unverified execution setting is assigned to the reference.

## Production methods

- `lifetimes.road_lifetime_survival_source` selects `nhtsa_nems` or `moves` for cars
  and trucks. Medium trucks share heavy-vehicle MOVES rates. Cumulative survival
  has an age-zero baseline followed by conditional annual retention products.
- `existing_capacity.road_stock_method` selects `stock_age` or `sales_survival` by
  province. `{ON: sales_survival}` covers an Ontario-only run. `{other: sales_survival}`
  independently reconstructs every selected province from its CEUD sales and calibrates
  utilization against its own CEUD activity and retained stock. Ontario age distributions
  do not enter these selected car/truck cohorts. Registration powertrain proxies,
  older-than-history uncertainty and fixed-lifetime reallocation remain audited.
- `nrcan_ceud_beyond_observed: constant` holds eligible CEUD endpoints through the
  base year. `extrapolated` uses the same observed-history CAGR method as optional
  car-demand extrapolation. Its shared YAML owner is `ceud_inputs.cagr`. Annual
  intervals touching 2020–2021 are excluded. Generated values never train the rate,
  zero endpoints remain zero and invalid projections or insufficient positive-valued
  history fail explicitly.
- CEUD extension covers stocks, sales, activity, distance, energy and consumption
  inputs. The separate road efficiency bridge aligns empirical NRCan/CEUD evidence
  and ATB in 2023. Annual 2024–2025 consumption follows ATB change ratios before
  cohort aggregation. A prospective first model label may equal the base year, keeping future vintage 2025 at its 2030 source year. Buses use
  their established distinct owner.
- Annual utilization remains vintage-indexed. Sales-survival mode uses fixed future
  lifetimes and flat utilization, with explicit unsupported-combination validation.
  Demand indexing uses real GDP. Monetary conversion retains its own deflator basis.

## Scenario execution

Run builds serially and capture each report before another build refreshes shared
audits. Each scenario owns a distinct database, validation report and persistent log.

| Scenario | Purpose |
| --- | --- |
| `validation_baseline.yaml` | Current production methods, observed 2023, Ontario stock-age control. |
| `researcher_sales_2023.yaml` | Provincial sales-survival with observed 2023 calibration. |
| `researcher_sales_2025.yaml` | Ontario sales-survival, base 2025, constant CEUD beyond 2023. |
| `researcher_sales_2025_extrapolated.yaml` | Matched 2025 run with observed non-pandemic CAGR extension. |
| `provincial_sales_2025.yaml` | Independent provincial stock/activity reconstruction throughout the configured geography. |

```text
uv run python src/build_transport.py --scenario config/scenarios/researcher_sales_2025.yaml --overwrite
uv run python -m validation.transportation_research --scenario config/scenarios/researcher_sales_2025.yaml --write-report
uv run python -m validation.charging_cost_comparison --scenario config/scenarios/researcher_sales_2025.yaml --write-report
uv run marimo run docs/insights/transportation_validation_diagnosis.py
```

After each native build, capture its full report before the next build changes shared
audits. Later comparison-code changes can refresh the derived database views using
`uv run python -m validation.transportation_research --scenario config/scenarios/researcher_sales_2025.yaml --refresh-comparisons --write-report`.
This verifies unchanged databases, configuration, reference evidence and captured
CSV hashes, preserves native audit bytes, and records the new comparison identity.

The new diagnosis notebook prioritizes significant stock, utilization, demand and
charger-cost differences. Supporting keys, source notes and complete exports stay
available in collapsible audits. Cost comparison reads supplied CSVs, harmonizes
million CAD2020 per PJ/year against current backend parameters, and labels inherited
baseline C2A, efficiency, utilization and lifetime context. Missing cost rows retain
missing status. Rated-capacity and utilization-normalized interpretations are explicit.

The real-GDP production demand calculation is assessed independently of any differently
labelled legacy growth spreadsheet. Comparison findings explain model/source/year and
representation differences rather than calibrating production parameters to the reference.

## Evaluation record

The observed-2023 control and constant/extrapolated-2025 Ontario databases passed
their native schema, provenance, support, integrity and foreign-key checks. A separate
registered-input test passed all 50 province/class stock/activity calibrations across
ten provinces, with maximum absolute residual 3.55e-15 billion service-km.

| Finding | Established result and interpretation |
| --- | --- |
| Constant-2025 stock | Relative to held CEUD totals, cars are -4.39%, passenger/freight light trucks +23.62%/+23.54%, medium trucks +61.88% and heavy trucks +45.47%. Annual sales history, survival and composition assumptions remain independently visible. |
| Vintage allocation | Fixed-lifetime eligibility redistributes 1.956 million vehicles, about 17.1% of reconstructed road stock. Car vintage 2010 differs by +120.06% from `validation_1`, 2015 by -5.65% and 2020 by -57.77%. This age redistribution is a material limitation of the current fixed-lifetime representation. |
| Activity and energy | Inserted-row activity closes to at most 2.84e-14 billion service-km. Independent vehicle-input energy is +5.75% to +11.33% above CEUD for cars/light/medium trucks and -0.17% for heavy trucks. The fleet/fuel and charging-loss boundaries differ. Activity closure establishes calibration consistency. |
| Charger investment, source 2030 | On the rated annual-capacity basis, current LDV/MHDV costs are 25.712/36.340 versus 52.577/164.326 million CAD2020 per PJ/year. Conditional normalization at backend utilization gives 128.560/121.134. Embedded reference utilization/loss conventions remain unresolved. |
| Inherited charger context | The original legacy baseline supplies efficiency 0.90 for both classes, compared with current 0.95/0.80. Both use 15-year life. Utilization, charger composition, monetary conversion and source timing contribute separate differences. |

The supplied SQLite retains identical values on all 99 shared non-charger transport
capacity keys and all 90 shared demand keys. Annual utilization is the substantial
saved difference, with 407 of 796 shared normalized keys changed, plus differing
key coverage. Stock summaries use one validated service-class crosswalk across the
three databases, so differing efficiency coverage cannot change class membership.
Notebook source changes and supplied database deltas are reported separately.
No unverified GDP interpretation or execution flag is used to tune the calculation.

The registered source now uses the August 2026 MOVES6 release, EPA-420-R-26-010,
Table C-1 on printed page 187/PDF page 189. The user-approved car age-27 annual
survival is 0.753, replacing 0.567 in MOVES5. The remaining 163 annual table cells
agree. This changes cumulative car survival from age 28 onward, with unchanged
fixed medians and sales-reconstruction values through age 25. Earlier captured
validation results retain their documented 2024 source identity.

Commands, focused checks, native build results, source-equality evidence and remaining
limitations are recorded in `.agents/plans/archive/130_production_bridge_and_validation_scenarios.md`.
The diagnostic exports contain database/configuration hashes and reject stale inputs.
Final focused regression passed 420 tests, with two opt-in lifecycle tests skipped.
The separate 18-test temporal suite, provincial calibration test and all three
diagnosis notebooks passed execution checks. Ruff and whitespace checks passed.

### MOVES6 and execution follow-up

The ten-province, constant-CEUD, base-2025 default scenario completed offline in
866.05 seconds on 4 November 2026. It published 222,135 contribution rows across
25 tables, passed 14 cross-table support checks and reported zero foreign-key
violations. All database tables, including provenance, match the completed build
before the lookup optimization at zero numerical tolerance. The persistent log is
`outputs/logs/baseline_default_2026-11-04_18-41-13-934903Z.log`.

Repeated rating matching, aggregation and provenance-table selection explain the
main observed cost. A matched ten-region 2023 composition benchmark fell from
370.49 to 93.96 seconds. All 8,384 tested provenance lookups retain their results
or errors, with runtime falling from 313.12 to 23.38 seconds. The final full build
spent 546.11 seconds on efficiencies, 12.17 seconds on insertion and 1.01 seconds
on final checks. Whole-run timings depend on concurrent work. Registered-source
replay, row validation and integrity checks remain part of every build.

The final integrated regression passed 842 tests, with four skips and two expensive
preparation cases deselected. Real native builds separately exercise the lifecycle.
Cancellation closes staging SQLite connections before cleanup. One audit-CSV atomic
replacement encountered a Windows access-denied error. A same-byte replacement and
the complete retry succeeded, preserving the previously published database on failure.
CodeRabbit's installer rejected this Windows environment and WSL was unavailable,
so no CodeRabbit review was produced. Detailed follow-up evidence is recorded in
`.agents/plans/archive/131_moves6_build_performance_and_bridge_diagnostics.md`.

The unchanged base-2025 diagnostic scenario rebuilt successfully and the complete
LDV notebook executed in about 91 seconds. Its CEUD index uses the configured
base-year extension locally. Source observation panels retain native observations.
Graph 4 joins prepared non-null-vintage endpoints with solid lines. Separate 2023
range anchors connect by dashed lines to the first matching future production row,
2030 in the reviewed 2023/2025 cases. Actual BEV/PHEV consumption and service-efficiency
renders were checked alongside the 78 focused diagnostic tests.

### Further performance evaluation

A second measured optimization pass completed the same full ten-province scenario
in 385.10 seconds, compared with 866.05 seconds above, a 55.5% reduction. Every SQLite
table, including provenance, matches at zero tolerance. The run published 222,135
contribution rows, passed all 14 support checks and reported zero foreign-key
violations. Its log is `outputs/logs/baseline_default_2026-11-04_19-57-00-318974Z.log`.

| Native stage | Previous completed run | Further optimized run |
| --- | --- | --- |
| All prerequisites | 93.27 s | 73.01 s |
| Existing capacity | 58.31 s | 21.75 s |
| Road utilization | 25.66 s | 4.74 s |
| Efficiencies | 546.11 s | 222.08 s |
| Rating composition, included in efficiencies | 343.71 s | 132.51 s |
| Vehicle costs | 101.43 s | 23.28 s |
| SQLite insertion and final checks | 13.18 s | 13.63 s |

The implementation removes repeated wide-table filtering, configuration matching,
median/schema work, technology-set construction and physical cost calculations.
Source-dependent caches belong to the preparation or evidence instance. Separate
real-input comparisons retain all classification/composition values, dtypes and row
order, cost rows and export bytes, and numerical returns and findings. The wider
regression passes 578 tests and the composition suite 333. Four cases skip
and two expensive preparation cases are deselected in the wider run. A separate
registered offline integration passes all 50 province/class activity calibrations,
with maximum residual 3.55e-15 billion service-km. Full replay and validated insertion
ran through the normal entrypoint. Whole-run timings vary with competing work.

Composition and source replay account for 132.51 and 73.01 seconds of the remaining
elapsed time. The task-specific measurements and rejected low-benefit experiments
are recorded in `.agents/plans/archive/132_deeper_native_build_performance.md`.

The user's base-2025 diagnostic scenario also completes through the normal builder
in 232.05 seconds, compared with 404.05 seconds in the prior pass. The scenario YAMLs
and both canonical vehicle CSVs match their task-start hashes. The complete fresh-capture
LDV app executes 29 outputs in 69.66 seconds, with its 2023 ATB reference and 2025
base year. Commands, checks and remaining limits are recorded with the measurements.
