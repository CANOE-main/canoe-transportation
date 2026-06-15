# 000 Strategic Roadmap Briefing

Date inspected: 2026-05-06

Purpose: concise repo-state briefing for designing the next strategic ExecPlan roadmap for the CANOE transportation backend v2.0 refactor.

## 1. Current Implemented Architecture

Implemented:

- Top-level guidance and planning: `AGENTS.md`, `.agents/PLANS.md`, `.agents/plans/001_config_control_layer.md`, `.agents/plans/002_stocks_and_demands_data_fetching.md`, `.agents/plans/003_ontario_vehicle_population_fetching.md`.
- Package/tooling: `pyproject.toml`, `uv.lock`, `uv.toml`, `README.md`; Python 3.13 with `pandas`, `pyyaml`, `requests`, `snakemake`, `sqlalchemy`, `pytest`, `ruff`, `openpyxl`, and `xlrd`.
- Config scaffold: `config/paths.yaml`, `config/sources.yaml`, `config/scenarios/legacy_reproduction.yaml`, `config/parameters/harmonization_rules.yaml`, `config/parameters/conversion_factors.yaml`.
- Python scaffold:
  - `src/setup.py`: config smoke entry point.
  - `src/build_transport.py`: placeholder only; it defines `main()` but does not call it, so `uv run python src/build_transport.py` exits successfully without compiling anything.
  - `src/parameterization/utils.py`: YAML loading, path resolution, config validation, directory creation.
  - `src/parameterization/nrcan_ceud.py`: fetch/cache/normalize NRCan CEUD transport Excel tables.
  - `src/parameterization/ontario_vehicle_population.py`: discover/cache/normalize Ontario vehicle population Reports 4 and 5.
  - `src/validation/config_smoke.py`: config smoke validation.
- Tests: `tests/test_config.py`, `tests/test_nrcan_ceud.py`, `tests/test_ontario_vehicle_population.py`.
- Workflow: `workflow/Snakefile` exists with `all`, `setup_smoke`, and `fetch_ontario_vehicle_population` rules.
- Inputs/artifacts:
  - `inputs/canoe_dataset_schema.sql` with 76 `CREATE TABLE` statements.
  - Cached NRCan Ontario CEUD `.xls` files under `inputs/cache/nrcan_ceud_transport/`.
  - Cached Ontario 2022 vehicle population ZIP under `inputs/cache/ontario_vehicle_population/`.
  - Interim CSV outputs under `inputs/interim/fetched_nrcan_ceud_inputs/` and `inputs/interim/fetched_ontario_vehicle_population/`.
  - Setup logs under `outputs/logs/`.

Partially implemented:

- `config/` exists and is usable, but only one active scenario exists: `config/scenarios/legacy_reproduction.yaml`.
- `inputs/cache/` and `inputs/interim/` contain real generated source-normalized data; `inputs/processed/` is empty.
- `outputs/logs/` contains setup smoke logs; `outputs/sqlite/` and `outputs/validation/` are empty.
- `src/validation/` has smoke validation only, not SQLite parity/schema/tolerance validation.
- Source modules produce interim data, but no parameter-ready tables or SQLite inserts.

Missing relative to `AGENTS.md` planned architecture:

- Most planned parameter modules are absent: `stocks_and_demands.py`, `on_road_effs_and_costs.py`, `on_road_variable_costs.py`, `off_road_effs_and_costs.py`, `on_road_lifetimes.py`, `off_road_lifetimes.py`, `ldv_charging_dist.py`, emissions modules, constraints modules, and `sector_coupling.py`.
- Planned static input CSVs are absent: `existing_techs.csv`, `new_techs.csv`, `fuel_commodities.csv`, `demands.csv`, `regions.csv`, `time.csv`.
- No SQLite build orchestration, schema instantiation command, data insertion layer, or post-processing layer exists.
- No modular `workflow/rules/` directory exists.
- No `.agents/skills/` directory exists.

Deviations:

- The legacy directory is `legacy_backend/`, not `legacy/`; configs correctly point to `legacy_backend/`.
- `workflow/Snakefile` currently fails to parse because `fetch_ontario_vehicle_population` mixes wildcarded outputs with a non-wildcard manifest output.
- The current `README.md` still describes the project as an early scaffold, but the repo now has two real interim source-normalization modules.
- `outputs/logs/setup_smoke_baseline.json` refers to a stale `config/scenarios/baseline.yaml` scenario that is not present.
- `config/scenarios/legacy_reproduction.yaml` active sources include NRCan provincial/national only; the Ontario vehicle population source is implemented but not active in the scenario.

## 2. Current ExecPlan History

`000_strategic_roadmap.md`

- Status: this briefing file; previously empty.
- Implemented outputs: this report.
- Validation commands run: see section 4.
- Unresolved placeholders: none inside this briefing.
- Recommended next step: split roadmap into focused ExecPlans, starting with workflow repair and schema instantiation.

`001_config_control_layer.md`

- Status: completed.
- Implemented outputs: `config/paths.yaml`, `config/sources.yaml`, `config/scenarios/legacy_reproduction.yaml`, `src/setup.py`, `src/build_transport.py`, `src/parameterization/utils.py`, `src/validation/config_smoke.py`, `workflow/Snakefile`, `tests/test_config.py`, `README.md`, packaging/test config.
- Validation recorded in plan: `uv run python src/setup.py --scenario config/scenarios/legacy_reproduction.yaml`, `uv run pytest`, `uv run ruff check .`, `uv build`, and Snakemake smoke passed at that time.
- Current unresolved placeholders: no SQLite instantiation, no source downloads in setup, no transformation/build, incomplete source metadata/checksums/citations, no parameter maps.
- Recommended next step from plan: schema instantiation; still not done.

`002_stocks_and_demands_data_fetching.md`

- Status: completed for an Ontario-only NRCan CEUD interim extraction slice.
- Implemented outputs: `src/parameterization/nrcan_ceud.py`, source metadata for CEUD provincial/national tables, harmonization rules, tests, cached Ontario CEUD files, interim manifest and normalized Ontario CSV.
- Validation recorded in plan: CEUD Ontario command, `uv run pytest`, `uv run ruff check .`.
- Current generated outputs: 17 Ontario CEUD cached files; `inputs/interim/fetched_nrcan_ceud_inputs/manifest.csv` with 17 rows; `nrcan_ceud_transport_on.csv` with 2,593 rows; empty `warnings.log`.
- Unresolved placeholders: national tables and other provinces not generated; source-normalized only; no processed stocks/demands tables; no legacy parity check.
- Recommended next step: transform CEUD interim records into parameter-ready demand, stock, utilization, and fuel-use tables with explicit target mappings.

`003_ontario_vehicle_population_fetching.md`

- Status: completed, including later config-boundary and pytest-runtime updates.
- Implemented outputs: `src/parameterization/ontario_vehicle_population.py`, Ontario CKAN source metadata, harmonization rules, kg-to-lb conversion factor, tests, README command note, cached 2022 ZIP, report 4/5 interim outputs.
- Validation recorded in plan: Ontario command passed, `uv run pytest` passed with 20 tests, `uv run ruff check .` passed; earlier sandbox temp-dir failures were resolved via repo-local pytest paths.
- Current generated outputs: `inputs/cache/ontario_vehicle_population/2022_vehicle_population_data.zip`; report 4 cleaned and EPA GVWR distribution CSV; report 5 cleaned and age distribution CSV; manifest; empty `warnings.log`. Current report 4 distribution has 9 rows; report 5 age distribution has 124 rows.
- Unresolved placeholders: not connected to downstream parameter modules; CKAN metadata is still queried even with `--no-download`; source is not active in `legacy_reproduction.yaml`; no SQLite or parity integration.
- Recommended next step: convert Ontario age/GVWR distributions into processed fleet-attribute inputs consumed by downstream on-road stock/cost modules.

## 3. Current Config/Control Layer

Existing YAML files:

- `config/paths.yaml`: canonical roots for config, inputs, cache, external, interim, processed, schema, outputs, SQLite, validation, logs, and legacy evidence paths.
- `config/sources.yaml`: source registry and source-component metadata for:
  - `nrcan_ceud_transport_provincial`
  - `nrcan_ceud_transport_national`
  - `ontario_ministry_transport_vehicle_population`
- `config/scenarios/legacy_reproduction.yaml`: scenario name, regions, model years, active sources, SQLite/report/log output names, validation reference SQLite, and switches.
- `config/parameters/harmonization_rules.yaml`: CEUD row/label cleanup rules, output naming, Ontario Report 4/5 filters, EPA GVWR bins, age filters, and output naming.
- `config/parameters/conversion_factors.yaml`: currently only `mass.kg_to_lb`.

Already configurable:

- Canonical paths and legacy reference paths.
- CEUD URL templates, cache filenames, source years, allowed/default regions, table IDs, labels, intended consumers, and source-native metadata.
- Ontario CKAN access metadata, package ID, resource selector, cache filename, report member templates, and report metadata.
- Scenario regions, model years, active sources, output filenames, and coarse switches (`legacy_equivalent`, `download_sources`, `compile_sqlite`, `transform_parameters`).
- CEUD parsing assumptions: skip rows, label cleanup, dropped noise rows, table 7 header behavior, table label prefix behavior.
- Ontario parsing assumptions: kept weight class, required columns, GVWR bins, kept descriptor/classes, max age.
- kg-to-lb conversion factor.

Still hardcoded or not yet controlled:

- Source IDs are Python constants in source modules.
- Snakemake target paths are hardcoded in `workflow/Snakefile`.
- `src/build_transport.py` has no real CLI/config behavior.
- Ontario `--no-download` still fetches CKAN metadata instead of using cached/resolved metadata.
- No config maps exist for target SQLite tables, primary keys, units, demand/technology/commodity mappings, static dimensions, source-to-parameter formulas, tolerances, or parity baselines.
- Legacy `compile_transport.py` hardcodes province/spreadsheet/profile names and reads many Excel sheets directly.
- No current RAMP/GREET/AEO/fuel-price source entries exist in `config/sources.yaml`.

## 4. Current Pipeline Capability

Commands verified during this inspection:

- `uv run python src\setup.py --scenario config\scenarios\legacy_reproduction.yaml`: passed; writes `outputs/logs/setup_smoke_legacy_reproduction.json`.
- `uv run pytest`: passed, 20 tests.
- `uv run ruff check .`: passed.
- `uv run python -m parameterization.nrcan_ceud --scenario config/scenarios/legacy_reproduction.yaml --regions ON --skip-national --no-download`: passed using cached CEUD files.
- `uv run python -m parameterization.ontario_vehicle_population --scenario config/scenarios/legacy_reproduction.yaml --year 2022 --no-download`: passed only outside the sandbox because it still needs a live CKAN metadata request before reusing the cached ZIP.
- `uv run python src\build_transport.py`: exits 0 but performs no work because `main()` is not invoked.

Known failing command:

- `uv run snakemake -n --snakefile workflow\Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1`: fails with `RuleException` because `fetch_ontario_vehicle_population` has wildcarded report outputs and a non-wildcard `manifest` output.

Artifacts currently generated:

- Cache: 17 NRCan Ontario CEUD `.xls` files for 2021; one Ontario vehicle population 2022 ZIP.
- Interim:
  - `inputs/interim/fetched_nrcan_ceud_inputs/manifest.csv`
  - `inputs/interim/fetched_nrcan_ceud_inputs/nrcan_ceud_transport_on.csv`
  - `inputs/interim/fetched_nrcan_ceud_inputs/warnings.log`
  - `inputs/interim/fetched_ontario_vehicle_population/manifest.csv`
  - Ontario report 4 cleaned/distribution CSVs
  - Ontario report 5 cleaned/distribution CSVs
  - `inputs/interim/fetched_ontario_vehicle_population/warnings.log`
- Logs: `outputs/logs/setup_smoke_legacy_reproduction.json`; stale `outputs/logs/setup_smoke_baseline.json`.
- Empty: `inputs/processed/`, `outputs/sqlite/`, `outputs/validation/`.

## 5. Current Validation Capability

Implemented validation:

- Config smoke checks load YAML, validate required keys, create configured directories, and check that schema/reference SQLite paths exist.
- Tests cover:
  - config loading and required keys;
  - setup smoke status;
  - CEUD URL/cache rendering, label cleanup, unit extraction, normalization, and table 7 behavior;
  - Ontario CKAN resource selection, request construction, ZIP member resolution, Report 4 GVWR binning, Report 5 age normalization, and cache reuse.

Not implemented:

- No SQLite schema instantiation validation.
- No table row-count comparison against `legacy_backend/canoe_on_12d_baseline.sqlite`.
- No parity checks against legacy spreadsheets/workbooks.
- No validation reports in `outputs/validation/`.
- No tolerance config for parameter comparisons.
- No key-integrity, unit-consistency, CFT-normalization, pruning, or post-processing validation.

Current known validation gap:

- Snakemake cannot currently be used as the reproducibility gate because the Snakefile fails to parse.

## 6. Legacy Dependencies Still Needed

Actively referenced by config/code/tests:

- `legacy_backend/canoe_on_12d_baseline.sqlite`: reference SQLite existence check.
- `legacy_backend/canoe_schema.sql`: registered legacy schema path.
- `legacy_backend/transportation[deprecated]/compile_transport.py`: registered compiler reference.
- `legacy_backend/charging_profiles[deprecated]`: registered charging profile evidence path.
- `legacy_backend/model_constraints[deprecated]`: registered constraints evidence path.
- `legacy_backend/transportation[deprecated]/get_nrcan_data.py`: used as parsing evidence in ExecPlan 002 and mirrored by current CEUD logic.
- `legacy_backend/transportation[deprecated]/on_vehicle_population/`: used as parser/test evidence in ExecPlan 003.

Important legacy artifacts for baseline reproduction:

- `legacy_backend/transportation[deprecated]/spreadsheet_database/CANOE_TRN_ON_v4.xlsx` and related provincial workbooks.
- `legacy_backend/transportation[deprecated]/canoe_trn_template.xlsx`.
- `legacy_backend/transportation[deprecated]/compiled_database/canoe_trn_on_vanilla4.sqlite`.
- `legacy_backend/charging_profiles[deprecated]/ramp_mobility/results/` and charging profile notebooks/scripts.
- `legacy_backend/model_constraints[deprecated]/trn_constraints_*.xlsx` and `update_constraints.py`.
- `legacy_backend/db_processing[deprecated]/update_database/*.py` and `to_temoa_v3/*.py`.
- `legacy_backend/transportation[deprecated]/autonomie_assessment/`, `fuel_consumption_ratings/`, `fuel_supply[deprecated]/`, and `greet_model/`.

Legacy behavior most important to reproduce:

- Excel sheet-to-SQLite table mappings in `compile_transport.py`.
- Technology, commodity, demand, lifetime, cost, efficiency, emissions, split, and constraint row construction.
- RAMP mobility conversion to `DemandSpecificDistribution` and `CapacityFactorTech`.
- Duplicate cleanup and post-processing behavior recorded in `db_processing[deprecated]/update_database/update_log.txt`.
- CEUD background-data parsing/injection behavior, now partly mirrored in `nrcan_ceud.py`.

## 7. Source/Data Integration Status

Integrated with real code:

- NRCan CEUD transportation provincial/national source registry and Ontario provincial fetch/normalize logic.
- Ontario Ministry of Transportation vehicle population CKAN discovery, ZIP cache reuse, Report 4 GVWR distribution, and Report 5 age distribution.

Scaffolded but not parameterized:

- Source metadata says CEUD products feed `stocks_and_demands` and some on-road efficiency/cost logic, but those modules do not exist yet.
- Ontario outputs are intended for `stocks_and_demands`, `on_road_effs_and_costs`, and `on_road_variable_costs`, but those modules do not exist yet.

Not currently integrated:

- RAMP-mobility charging profiles.
- GREET/fuel-cycle and vehicle-cycle emissions.
- Autonomie/fuel-consumption rating sources.
- Fuel supply/fuel price futures.
- Market/adoption/lifetime constraints.
- Legacy workbook extraction maps.
- Static technology/commodity/demand/time/region dictionaries.

Roadmap-ready generated products:

- CEUD Ontario source-normalized interim table can become the first `stocks_and_demands` processed-data milestone.
- Ontario 2022 age distribution can become the first fleet survival/stock initialization milestone.
- Ontario 2022 EPA GVWR distribution can become the first medium/heavy truck disaggregation milestone.

## 8. Roadmap Risks And Blockers

Top technical blockers:

- Snakemake is present but currently unusable due the invalid Ontario rule output wildcard contract.
- There is no SQLite instantiation/build layer despite a schema file and output path existing.
- There are no target table mappings from interim data to CANOE/Temoa tables.
- No processed data layer exists between source-normalized interim files and future SQLite insertion.
- Validation is unit/smoke-level only; there is no parity harness.
- Legacy compiler behavior is large, Excel-centered, and highly implicit.

Top ambiguity/assumption blockers:

- Baseline reproduction scope is not yet narrowed to a first table/parameter slice.
- Legacy source years and update policy need explicit treatment: CEUD uses 2021, Ontario uses 2022, while the scenario model years run 2021-2050.
- Technology/commodity/demand naming and class mappings are still mostly inherited from legacy spreadsheets, not config-owned.
- Units and conversion targets for Temoa tables are not mapped.
- RAMP charging and post-processing behavior affect baseline parity but are not yet registered as reproducible v2.0 sources.

Suggested ordering for the next ExecPlans:

1. Workflow health and command contract: fix `workflow/Snakefile` parse failure, add explicit cached/offline targets, and make Snakemake the reliable smoke gate.
2. SQLite schema instantiation and schema smoke validation: create an empty scenario SQLite from `inputs/canoe_dataset_schema.sql` and validate expected tables/keys before inserting data.
3. Static dimension bootstrap: register or extract minimal Region, TimePeriod, Technology, Commodity, Demand, DataSource, and related dictionaries needed by the first baseline slice.
4. CEUD-to-processed stocks/demands slice: transform `nrcan_ceud_transport_on.csv` into explicit processed demand/stock/utilization/fuel-use tables with source provenance and tests.
5. Ontario fleet attributes slice: transform Report 5 age shares and Report 4 GVWR shares into processed fleet-attribute inputs consumed by on-road modules.
6. Minimal SQLite insertion and row-count validation for one slice: insert the static dimensions plus one CEUD-derived parameter family and compare row counts/keys to the legacy SQLite where possible.
7. LDV charging profile integration: register RAMP mobility outputs and reproduce the legacy `DemandSpecificDistribution`/`CapacityFactorTech` path because charging affects baseline structure and time-slice behavior.
8. Legacy compiler parity map: document the remaining Excel sheet-to-table functions from `compile_transport.py` into prioritized parameter-family ExecPlans for costs, efficiencies, lifetimes, emissions, constraints, and sector coupling.
