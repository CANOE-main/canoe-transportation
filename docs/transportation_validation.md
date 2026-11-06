---
title: Transportation validation and scenario controls
role: Production scenario methods and independent legacy parameter comparisons.
status: implementation_and_evaluation
retrieve_when: Reviewing provincial sales-survival scenarios or legacy parameter comparisons.
verify: Current executable configuration and generated reports govern numerical findings.
---

# Transportation validation

The backend uses registered production evidence and the current canonical vehicle
reference tables. The files under `legacy_backend/legacy_variant_sales_cohorts/`
and `legacy_backend/legacy_vatiant_charger_costs/` are read-only comparison evidence
(formerly `validation_1` and `validation_2`; the charger spelling is intentional).
Their values never enter source registration,
production calculations or database insertion.

`legacy_variant_sales_cohorts/canoe_ON_16d.sqlite` is the supplied Ontario extract of the notebook-derived
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

| Surviving scenario | Role in validation |
| --- | --- |
| `validation_baseline.yaml` | Both named legacy-variant comparisons: Ontario prospective 2025, sales-survival stock calibrated to CEUD, constant historical extension, MOVES fixed lifetimes and representative LDV ranges. |
| `legacy_comparison.yaml` | Ten-region legacy-period comparison; NHTSA/NEMS survival curves. Its selections differ from the production default. |
| `default.yaml` | Canonical ten-region prospective-2025 production selection, stock-age/MOVES/fixed lifetimes and explicit future range shares. |
| `notebook_diagnostics.yaml` | Ontario prospective-2023 recorded vehicle-stock/efficiency evidence; independent of the historical sales-survival reports below. |

The [scenario convention](../config/scenarios/README.md) owns current roles and temporary
full-YAML variants. The former `researcher_sales_2023`, `researcher_sales_2025`,
`researcher_sales_2025_extrapolated` and `provincial_sales_2025` names in prior records
identify historical experiments; they are no longer supported preset files. Their
reported modeling selections and captured evidence must not be inferred from a surviving
scenario with a similar name or base year.

```text
uv run python src/build_transport.py --scenario config/scenarios/validation_baseline.yaml --overwrite
uv run python -m validation.transportation_research --scenario config/scenarios/validation_baseline.yaml --write-report
uv run python -m validation.charging_cost_comparison --scenario config/scenarios/validation_baseline.yaml --write-report
```

These commands regenerate both comparisons from `validation_baseline.yaml`. Capture
the full fleet report before another build changes shared audits. The charging report
uses one shared latest `snapshot.json`; the notebook rejects captures from different
scenarios or database/build fingerprints.

To view the currently selected existing captures without generating reports:

```text
uv run marimo run docs/insights/transportation_validation_diagnosis.py
```

The diagnosis notebook resolves the live YAML selected by
`transportation_research.notebook_scenario`, verifies its fresh build and both captures,
and checks exported table hashes. It does not load a deleted scenario's historical report.

`--refresh-comparisons --write-report` is stricter: it requires the original effective
scenario, database, build configuration and reference identities, and retains native audit
bytes. Renamed evidence paths or deleted scenario recipes may prevent such a refresh;
do not relabel a historical capture or substitute the default to bypass those checks.

The new diagnosis notebook prioritizes significant stock, utilization, demand and
charger-cost differences. Supporting keys, source notes and complete exports stay
available in collapsible audits. Cost comparison reads supplied CSVs, harmonizes
million CAD2020 per PJ/year against current backend parameters, and labels inherited
baseline C2A, efficiency, utilization and lifetime context. Missing cost rows retain
missing status. Rated-capacity and utilization-normalized interpretations are explicit.

The real-GDP production demand calculation is assessed independently of any differently
labelled legacy growth spreadsheet. Comparison findings explain model/source/year and
representation differences rather than calibrating production parameters to the reference.

## Historical evaluation record

The following numerical findings describe the completed Plan 130/131 evaluations, not
a fresh comparison of the current default. Original captured configurations and source
editions govern their interpretation.

The observed-2023 control and constant/extrapolated-2025 Ontario databases passed
their native schema, provenance, support, integrity and foreign-key checks. A separate
registered-input test passed all 50 province/class stock/activity calibrations across
ten provinces, with maximum absolute residual 3.55e-15 billion service-km.

| Finding | Established result and interpretation |
| --- | --- |
| Constant-2025 stock | Relative to held CEUD totals, cars are -4.39%, passenger/freight light trucks +23.62%/+23.54%, medium trucks +61.88% and heavy trucks +45.47%. Annual sales history, survival and composition assumptions remain independently visible. |
| Vintage allocation | Fixed-lifetime eligibility redistributes 1.956 million vehicles, about 17.1% of reconstructed road stock. Car vintage 2010 differs by +120.06% from the supplied cohort reference, 2015 by -5.65% and 2020 by -57.77%. This age redistribution is a material limitation of the current fixed-lifetime representation. |
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


## Source and diagnostic follow-up

[ExecPlan 131](../.agents/plans/archive/131_moves6_build_performance_and_bridge_diagnostics.md)
records the separately authorized MOVES6 source correction and its source-value checks.
The base-2025 diagnostic review kept native CEUD observations distinct from the configured
extension and separated prepared production values from ATB indexing references. These
are interpretation boundaries, not calibration against the supplied legacy values.

Backend build timings, algorithmic changes, optimization-only exact SQLite/provenance
comparisons, failure-recovery findings and measurement limitations are consolidated in
[the codebase diagnostic snapshot](codebase_diagnostic_snapshot.md#historical-performance-evidence).
They establish engineering regression evidence; they do not independently establish legacy
parity, solver feasibility or scientific validity of the transportation methods above.
