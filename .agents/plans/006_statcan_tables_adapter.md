# 006 Statistics Canada Tables Adapter

## Goal

Add a scenario-aware Statistics Canada Web Data Service adapter that fetches, caches,
normalizes, and manifests the five configured transport tables needed by later stock,
lifetime, and road-aggregation modules.

## Context

- Existing fetchers use config-driven cache/interim paths, direct module entrypoints,
  offline cache reuse, per-source normalization, manifests, and warning logs.
- The workflow currently contains only the repo-doctor smoke rule.
- Official WDS cube metadata was inspected before selector design. It confirms:
  - `20-10-0021-01` is archived annual data for 2011-2021 with geography, fuel,
    vehicle type, and number-of-vehicles dimensions.
  - `20-10-0025-01` is current quarterly data beginning in 2017 with the same core
    dimensions plus an all-ZEV member and province/sub-provincial geography levels.
  - `23-10-0308-01` is annual from 2017, with exact LDV, medium-duty, class-7,
    class-8, bus, motorcycle, fuel, and number-of-vehicles members.
  - `34-10-0254-01` is occasional 2016/2018/2020 data in years, with six fuel-labelled
    bus useful-life asset members plus non-bus transit assets.
  - `23-10-0142-01` covers 2011-2017 and exposes origin, destination, mode,
    12 commodity groups, and shipments/weight/distance/tonne-kilometre measures.
- Full-table archives are resolved through the official WDS
  `getFullTableDownloadCSV` endpoint; metadata comes from `getCubeMetadata`.

## Scope

1. Register one StatCan source family and five table components in `sources.yaml`.
2. Add region/geography maps, exact table selectors, output names, LDV overlap policy,
   and freight rules to `rules.yaml`; add mile-kilometre conversion to conversion YAML.
3. Implement API metadata/download resolution, authoritative ZIP caching, offline
   reuse, source-contract validation, normalized table outputs, LDV history, and
   filtered freight records with derived average shipment/gross-weight fields.
4. Preserve reference period, geography, table/product IDs, units, scalar factors,
   vector/coordinate/status fields, and cache/member provenance.
5. Add focused network-free tests, a short README entrypoint/output note, and the
   smallest StatCan workflow rule consistent with the current Snakefile.

## Non-goals

- Do not implement stocks, lifetimes, road aggregation weights, parameter-ready data,
  SQLite loading, parity checks, or unrelated workflow refactors.
- Do not aggregate final regional/long-haul weights or generalize all StatCan tables.

## Implementation steps

1. Add source/rule/conversion/scenario configuration and request dataclasses.
2. Fetch/cache metadata JSON and full-table English ZIPs; fail offline when either
   authoritative artifact is absent.
3. Discover the main CSV member, validate configured dimensions/members, normalize
   names/provenance, and filter exact configured provincial geographies.
4. Sum quarterly current LDV values to annual observations; prefer current data for
   overlap because it is current while `20-10-0021-01` is archived, use archived data
   only before current coverage, and emit tested overlap diagnostics.
5. For freight, keep Truck (for-hire), all commodities, and rows whose origin or
   destination maps to a selected scenario province. Pivot required measures, apply
   configured scalar multipliers, derive average shipment weight/distance and gross
   vehicle weight, filter at 14,970 kg, and classify exactly 350 miles as regional.
6. Write all normalized outputs, a manifest, and warnings; add CLI and workflow wiring.
7. Run focused/full tests, offline command, and one live API smoke fetch.

## Validation

```powershell
uv run python scripts/doctor.py
uv run ruff check .
uv run pytest tests/test_statcan_tables.py
uv run pytest
uv run python -m fetching.statcan_tables --scenario config/scenarios/legacy_reproduction.yaml --no-download
```

Also run one live command without `--no-download`, inspect all five generated outputs,
and run the minimal Snakemake target/dry-run relevant to the new rule.

## Acceptance criteria

- Scenario regions control province selection for all tables; no province is embedded
  in Python.
- Five raw ZIP/metadata pairs support deterministic live and offline execution.
- Normalized outputs retain required provenance and fail on changed dimensions.
- LDV overlap and freight boundary/weight behavior are explicit, configured, tested,
  and warning/manifest-visible.
- Focused/full tests, Ruff, doctor, offline execution, and live smoke pass.

## Progress

- [x] Inspected repository instructions, prior plans, diagrams, fetchers/tests, config,
  scenario, runtime utilities, and current workflow.
- [x] Inspected official WDS documentation, cube metadata, dimensions, members,
  archive status, coverage, units, and full-download resolution for all five tables.
- [x] Implemented configuration, adapter, tests, README note, and workflow rule.
- [x] Ran live fetch and refined member/origin contracts against actual CSV artifacts.
- [x] Ran and recorded complete validation.

## Outcomes

Implemented `fetching.statcan_tables` with official WDS metadata/download resolution,
atomic metadata/ZIP caching, chunked reading, strict contract validation, scenario
province filtering, five normalized table outputs, explicit annual LDV reconciliation,
freight candidate derivation, and manifest/warning artifacts. Added 14 focused tests,
README usage/output notes, and one minimal Snakemake rule.

Generated cache artifacts: five `{product_id}-eng.zip` archives and five
`{product_id}-metadata.json` files under `inputs/0_cache/statcan_transport/`.
Generated interim artifacts under `inputs/1_interim/fetched_statcan_transport/`:

- five source-normalized table CSVs;
- `statcan_ldv_registrations_historical.csv` (360 rows, 2011-2025);
- `statcan_ldv_registrations_overlap.csv` (120 comparable 2017-2021 series);
- `statcan_23100142_freight_heavy_truck_candidates.csv` (7,458 rows);
- `manifest.csv` (five successful source rows) and `warnings.log`.

Live evidence: the current 2026 LDV year has only one quarter, so 24 incomplete annual
series were excluded. Of 120 archived/current overlap observations, 61 differ; the
current annualized values take precedence and the differences remain in the overlap
artifact. Freight candidates contain 5,317 long-haul and 2,141 regional rows with
source/cache provenance retained. Bus useful-life observations cover 2016, 2018, 2020.
Those candidate row counts are diagnostic only: `rules.yaml` explicitly requires
future haul-class shares to sum `tonne_kilometres` by `haul_class` and normalize the
tonne-kilometre totals.

Validation results:

- `uv run python scripts/doctor.py`: passed, no mutation.
- `uv run ruff check .`: passed.
- `uv run pytest tests/test_statcan_tables.py`: passed, 14 tests.
- `uv run pytest`: passed, 44 tests.
- `uv run python -m fetching.statcan_tables --scenario config/scenarios/legacy_reproduction.yaml --no-download`: passed using all ten cached artifacts.
- Live WDS full-download resolution through the implemented adapter: passed.
- `uv run snakemake -n --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1`: passed.

Remaining uncertainties: the 61 overlap differences require downstream interpretation
before parity claims; the current quarterly table will continue to grow; and freight
data end in 2017 and represent aggregate for-hire shipment groups, so final haul weights
must document temporal use and aggregation in `road_aggregation.py`.

Recommended next milestone: implement the first processed `stocks_and_demands.py`
slice combining CEUD stock/demand anchors with the reconciled StatCan LDV history and
medium-truck fuel distribution. Keep bus lifetime conversion and final freight haul
weights in later dedicated `lifetimes_survival.py` and `road_aggregation.py` plans.

## Decision log

- Use WDS full-table English CSV ZIPs plus cached cube metadata JSON. This is the
  documented official bulk-table API path and supports reproducible offline runs.
- Quote scenario region codes such as `ON` in YAML; PyYAML otherwise interprets this
  YAML 1.1 boolean token as `True`, defeating explicit geography resolution.
- Provisional LDV policy: current `20-10-0025-01` annualized quarterly observations
  take precedence in overlapping years; archived `20-10-0021-01` only backfills years
  before current coverage. Preserve overlap comparisons and warn on differences.
- Freight boundary policy is configurable and initially assigns exactly 350 miles to
  `regional`; long-haul is strictly greater than the converted boundary.
- Regional/long-haul distribution weights must use summed tonne-kilometres, not
  candidate row counts. The fetcher retains and validates that configured measure;
  final share calculation remains in the future `road_aggregation.py` slice.
- Live archive inspection showed that language appears in the ZIP filename but the
  internal data member is `{product_id}.csv` beside `{product_id}_MetaData.csv`.
  Discover the exact PID data member and do not infer it from the outer ZIP suffix.
- Freight cube metadata names the origin dimension descriptively, but the real CSV
  stores it in `GEO`/`DGUID`; normalize `GEO` to the semantic origin-geography field
  and preserve the separately named destination dimension.
- Actual bus useful-life CSV coverage includes 2018 between the metadata start/end
  years; record 2016/2018/2020 rather than inferring observations from endpoints.
- Live overlap evidence supports retaining diagnostics: precedence is deterministic,
  but differing values are not treated as equivalent or silently overwritten.
