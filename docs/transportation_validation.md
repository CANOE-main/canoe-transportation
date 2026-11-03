---
title: Transportation validation findings
role: Implemented researcher comparisons and reproducible evidence for the unified MVP documentation.
status: evaluated_with_reference_and_representation_limits
retrieve_when: Reviewing the researcher fleet reconstruction or charging-cost comparison.
read_scope: Read the findings and limitations, then follow the owning scenario and report commands.
verify: Executable configuration, registered evidence and generated reports outrank this narrative.
evaluated_on: 2026-10-29
repository_commit: 06a1b79b9ae81a1708de5d4d07ad3c49e5cc3b11
---

# Transportation validation findings

The backend now supports a regional sales-survival fleet reconstruction, explicit
CEUD endpoint holding, activity calibration and reproducible researcher comparisons.
The unchanged control is numerically equivalent to the pre-change implementation.
**Exact parity with the first researcher's modified scenario is not established:**
the supplied SQLite is an older stock-based state, and the notebook's numeric inputs
are missing. The new presets are a supported interpretation of its modelling ideas.
They are diagnostic scenarios, not independently validated fleet forecasts.

Implementation and execution decisions are recorded in
[ExecPlan 121](../.agents/plans/121_transportation_research_validation.md).
This document replaces the original proposal with what the code and evidence establish.

## Reference evidence and decisions

The immutable files are registered as comparison-only artifact routes in
[paths.yaml](../config/paths.yaml); they do not parameterize production or acquire
invented external-source DQ identifiers. Report manifests retain their SHA-256 identities.

| Evidence | Established state |
| --- | --- |
| `legacy_backend/validation_1/canoe_ON_16d.sqlite` | Integrity passes, no foreign-key violations. All 99 common non-charger transport capacity rows, 90 demand rows and 122 lifetime rows match the older `canoe_on_12d_baseline.sqlite`. Seven charger capacities differ. Its survival table is empty. |
| `Update data_20260618_fixed lifetime (1).ipynb` | Cell 12 aggregates raw annual sales times powertrain shares, without prior survival. Cells 21/23 calibrate **activity**, not energy, using representative-vintage survival in 2023. The notebook constructs tables/CSVs but does not write the supplied SQLite. |
| Notebook timing | Cell 12 endpoint-holds 2024–25 sales and includes them in a 2023 calibration. Its bins assign 2005 to vintage 2000 and 2021–25 to 2020. Cell 29 shifts demand labels five years backward. These conventions are inspected and quantified, not silently adopted. |
| Notebook execution state | Current cell 31 selects `fixed_lifetime=False`; saved output describes the fixed branch. Filename and saved output do not establish the executed method. |
| Missing inputs | `vehicle_sales_by_size.xlsx`, `vehicle_sales_by_powertrain.xlsx`, `survival_curve_transport.xlsx`, `c2a_vehicle.xlsx`, `activity.xlsx`, `demand_growth_rate.xlsx`, and original `Input data/canoe_ALL_cm.sqlite` were not found. Their values cannot be inferred from the supplied database. |
| Charging CSVs | The user confirmed **million 2020 CAD per PJ/year of charging throughput**. Their embedded utilization/loss conventions and complete derivation remain undocumented. |

The planning proposal's energy calibration and linear CEUD extrapolation were not
implemented. The notebook supports activity closure and endpoint holding; it supplies
no defensible linear fitting window or slope. This also avoids introducing a circular
capacity/efficiency calibration dependency. Energy is an independent accounting check.

## Fleet reconstruction

Three complete Ontario presets separate the method and later-base-year effects:

| Preset | Fleet basis | CEUD anchor | Active model labels |
| --- | --- | --- | --- |
| [validation_baseline.yaml](../config/scenarios/validation_baseline.yaml) | Existing stock/age method | Observed 2023 | 2025–2045 |
| [researcher_sales_2023.yaml](../config/scenarios/researcher_sales_2023.yaml) | Annual sales × cumulative survival | Observed 2023 | 2025–2045 |
| [researcher_sales_2025.yaml](../config/scenarios/researcher_sales_2025.yaml) | Annual sales × cumulative survival | 2023 CEUD held through 2025 | 2030–2045 |

`existing_capacity.road_stock_method` is a regional map with an `other` fallback:
`stock_age` retains the previous calculation; `sales_survival` applies to cars,
passenger/freight light trucks and medium/heavy trucks. Motorcycles and buses retain
their existing owners. The implementation supports regional overrides and a shared
fallback; complete native execution here covers Ontario. Pure mixed-region tests
also verify that selecting one region leaves the other's stock calculation unchanged.

For sale year `y` and base year `b`, annual surviving capacity is
`sales[y] × S(b − y)`, in thousand vehicles. Age-zero survival is one. The selected
NHTSA/MOVES/NEMS evidence is reused; it is not refitted. MTO age distributions do not
enter this calculation, although MTO class weights still aggregate LDV survival.
The supplied inputs start in 2000. Missing interior sales years or unsupported
survival ages fail; older unobserved sales are neither invented nor treated as zero.

CEUD does not supply the required powertrain sales breakdown. Existing registered
fuel shares, geographic/early-year proxies, supported-fuel exclusions and the
medium-truck BEV override remain explicit allocation assumptions. Reconstructed
stock is **not rescaled** to CEUD stock.

| Ontario class | CEUD 2023 stock | Sales-survival 2023 | Sales-survival 2025 | 2025 difference from held CEUD |
| --- | ---: | ---: | ---: | ---: |
| Cars | 3,605.267 | 3,517.171 | 3,230.991 | −10.38% |
| Passenger light trucks | 4,355.458 | 4,897.178 | 5,324.685 | +22.25% |
| Freight light trucks | 1,215.632 | 1,365.733 | 1,485.112 | +22.17% |
| Medium trucks | 421.755 | 597.109 | 654.305 | +55.14% |
| Heavy trucks | 143.136 | 187.341 | 201.210 | +40.57% |

All stocks are **thousand vehicles**. Differences are results of coverage, sales,
transferred survival and composition assumptions; they are not measured errors or
quantitative uncertainty rankings. Endpoint holding keeps reported stock/activity
constant, while annual survival and two additional years of held sales change the
reconstructed fleet.

### Lifetime and period qualification

The current future survival rows contain unconditional survival since manufacture.
Combining them with already-surviving sales would apply historical retirement twice.
The new method therefore requires fixed future lifetimes and flat utilization.
Current medians are 14/16/16/18/19 years for the five classes above, matching the
supplied legacy fixed lifetimes. Conditional remaining-life curves would need a
separate validated interface before supporting that combination.

The established fixed-lifetime eligibility policy conserves each technology total
by reallocating ineligible stock to its eligible vintages. This is retained and
quantified, not mistaken for a second survival multiplication. **In the 2025 preset,
81.1% of car stock and approximately 30.6–39.7% of the four truck classes move across
annual cohorts.** The first model label is 2030 because the existing temporal
contract requires model labels after the base year. Coarse vintage labels and fixed
median lifetimes materially distort ages; this scenario must not be presented as
an independently validated age distribution or retirement forecast.

### Activity and energy

For each selected regional class, utilization is base-year CEUD activity divided by
retained reconstructed capacity times manual C2A. The same level applies to existing
and new vintages. Other classes retain the mean of the latest five eligible **observed**
CEUD activity/stock ratios; held years do not enter that mean. Zero denominators and
factors outside [0, 1] fail. Calibration uses retained rows after cleanup and carries
their full source/provenance contributors into utilization.

Activity closes within approximately `3e-14` billion service-km. This is a calibration
identity, not independent validation. The following 2025 accounting uses inserted
cohort efficiencies and counts PHEV blend input once:

| Class | CEUD secondary energy, PJ | Implied vehicle input, PJ | Difference |
| --- | ---: | ---: | ---: |
| Cars | 133.433 | 161.751 | +21.22% |
| Passenger light trucks | 220.448 | 253.600 | +15.04% |
| Freight light trucks | 74.905 | 84.733 | +13.12% |
| Medium trucks | 57.541 | 61.712 | +7.25% |
| Heavy trucks | 157.010 | 154.572 | −1.55% |

This is vehicle-input accounting, excluding upstream charger losses, not optimized
operation. Current cohort-average efficiency evidence, registration proxies,
unsupported fuels, fixed-lifetime redistribution and CEUD's fleet-energy boundary
prevent exact agreement. Those residuals are retained rather than calibrated away.
The exercise does not apportion every residual uniquely among those drivers.

### CEUD extension and notebook sensitivity

`sources.nrcan_ceud_beyond_observed: error | constant` separates source edition from
the parameter base year. Native normalized observations remain unchanged.
`parameterization.ceud_inputs` recomputes parameter views from them and writes separate
observed/held labels, source endpoints and hashes. Only current series tails extend;
interior gaps and discontinued historical fuel series are not filled. No values are
extended across the entire model horizon, and CER projections remain independently
selected.

The Ontario-plus-national 2025 build adds **312 labeled rows** across 156 series and
explicitly skips 14 historical fuel series. A read-only ten-province-plus-national
check preserves every native cell and yields 2,382 held rows, with 140 historical
series skipped.

The report also replays notebook binning with **current registered inputs**, not the
missing workbooks. Future 2024–25 sales contribute approximately 212.1 thousand cars,
830.0 thousand passenger light trucks and 231.7 thousand freight light trucks to its
2023 active-stock calculation. Older representative-bin ages introduce an offsetting
downward bias. For cars, their net effect gives 2,799.5 thousand versus 3,238.1 thousand
true annual-cohort survivors over the same 2005-start comparison window. This
separates look-ahead and binning effects rather than assuming their net bias is positive.

## Charging infrastructure costs

The reference has 24 investment rows and 69 fixed-cost rows for Ontario. It separates
LDV BEV/PHEV and MDV/HDV chargers; the backend pools them into LDV and MHDV infrastructure.
The reference notes describe US Borlaug assumptions: equally weighted LDV L2
home/public/DCFC and truck DCFC/transmission upgrades. The backend uses reviewed
Canadian Dunsky costs plus US residential L1. Their detailed boundaries are different.

Comparison matches **source calendar years**, not nominal vintage labels: prospective
backend vintage 2025 uses 2030 cost inputs. Its 2025 reference investment rows therefore
have no matching new-capacity key in this preset; they remain missing, not zero.

| Source year/class | Researcher | Backend capacity-normalized | Backend difference |
| --- | ---: | ---: | ---: |
| 2030 LDV | 52.577 | 25.712 | −51.10% |
| 2035 LDV | 52.577 | 21.646 | −58.83% |
| 2040–2050 LDV | 52.577 | 21.603 | −58.91% |
| 2030 MHDV | 164.326 | 36.340 | −77.89% |
| 2035 MHDV | 164.326 | 34.788 | −78.83% |
| 2040–2050 MHDV | 164.326 | 33.586 | −79.56% |

Values are million 2020 CAD per PJ/year of capacity. Backend per-GW costs divide by
the configured physical factor **31.536 PJ/year per GW**. This dimensional conversion
does not establish equal delivered-throughput economics. At backend target utilization
0.20/0.30, the 2030 rates become **128.560 LDV / 121.134 MHDV** per effective PJ/year:
**+144.52% / −26.28%** relative to the reference. Reference utilization and loss
conventions remain unknown, so these are a separately labeled sensitivity.
Backend efficiencies are 95%/80%; no undocumented reference loss correction is applied.
No discounting, lifetime division or annualization is hidden in the investment comparison.

Both sources use annual fixed costs of **1% of investment** for matched new vintages.
The reference has nine historical fixed-cost rows without corresponding investment
entries. Existing/new representation, the backend's 15-year infrastructure lifetime,
source-year mapping and horizon account for unmatched fixed-cost keys; they are not
filled. `CostVariable_ALL.csv` contains 11,937 rows but **no mapped charger rows**;
the backend likewise has no charging variable-cost entries. Absence is not an explicit zero.

A material backend limitation was identified: reviewed per-GW rates are averaged
with **port-count shares** (LDV 12/87/1%; MHDV 82/16/2%). Total expenditure divided by
total rated GW would instead require power weighting. The current arithmetic was
preserved; the full per-component monetary conversion is exported. Review that
aggregation boundary before adopting an economic conclusion or changing assumptions.
The researcher costs, utilization or charging profiles were not adopted.

## Reproduction and completion boundary

Run builds **serially**, capturing each research report before the next build changes
shared generated audits. The normal command is offline; `--overwrite` only replaces
that preset's distinct database:

```text
uv run python src/build_transport.py --scenario config/scenarios/validation_baseline.yaml --overwrite
uv run python -m validation.transportation_research --scenario config/scenarios/validation_baseline.yaml --write-report
uv run python -m validation.charging_cost_comparison --scenario config/scenarios/validation_baseline.yaml --write-report

uv run python src/build_transport.py --scenario config/scenarios/researcher_sales_2023.yaml --overwrite
uv run python -m validation.transportation_research --scenario config/scenarios/researcher_sales_2023.yaml --write-report

uv run python src/build_transport.py --scenario config/scenarios/researcher_sales_2025.yaml --overwrite
uv run python -m validation.transportation_research --scenario config/scenarios/researcher_sales_2025.yaml --write-report
```

Native databases/reports use each scenario's configured output names. Persisted
[research evidence](../outputs/validation/transportation_research/) contains per-scenario
stock/activity/energy tables, annual cohort allocations, extension audits, notebook
source cells and method sensitivity. The [charging comparison](../outputs/validation/charging_cost_comparison/)
retains original values, matched values, component conversions, missing/zero status
and manifests. Each report records configuration, database/reference hashes, commands,
tolerances and source status. Freshness gates reject stale cohort, CEUD or survival
evidence. Recorded paths identify the execution workspace; exported tables and hashes
remain the captured evidence after shared intermediates change.

Validation used isolated raw-input workspaces with network connections denied, preserving
live generated/user work. The control agrees across all **88 numerical model tables**
at absolute `1e-9` / relative `1e-12`, excluding provenance identities; provenance/DQ
are validated independently. Native scenarios pass schema insertion, 14 applicable support
checks, provenance/DQ, integrity and foreign keys. Focused tests cover formula/coverage,
regional isolation, activity feasibility, source preservation, report freshness and
cost units; exact commands/results are in the ExecPlan.

Completion means the supported methods compile reproducibly, accounting reconciles,
defaults remain equivalent, and differences and missing evidence are visible. It does
**not** mean exact modified-notebook numerical parity, accepted researcher assumptions,
validated age/energy forecasts, or a system-optimization result. Missing workbooks and
a genuine modified output are needed for exact parity; conditional future survival,
age redistribution and charger cost aggregation are material follow-up decisions for
[the subsequent unified MVP documentation](mkdocs_scope_and_implementation.md).
