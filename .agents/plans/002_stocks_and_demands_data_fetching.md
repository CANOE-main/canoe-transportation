# 002_stocks_and_demands_data_fetching.md

## Goal

Build the first executable v2.0 data layer for NRCan CEUD transport inputs.

This task replaces the legacy spreadsheet-injection step with a reproducible workflow that:

1. reads NRCan CEUD source metadata from `config/sources.yaml`;
2. downloads missing CEUD transport `.xls` tables into cache;
3. parses and normalizes them into traceable long-form interim files;
4. writes outputs under `inputs/interim/fetched_nrcan_ceud_inputs/`.

Do not build SQLite outputs yet. Do not recreate or modify legacy Excel workbooks.

## Context

The legacy `get_nrcan_data.py` is useful only as parsing evidence. It downloaded CEUD tables, cleaned row labels, removed Shares/GHG rows, concatenated tables, and inserted results into CANOE_TRN_ON_v4 Excel workbooks.

In v2.0, instead, the inputs are fetched, cleaned and harmonized such that later parameter modules can use these. For now, we're testing the fetching process such that I can see the interim, transformed tables and how they're being structured for subsequent use.

## Files likely involved

- `config/sources.yaml`
- `config/paths.yaml` if path additions are needed
- `src/setup.py`
- `src/parameterization/utils.py`
- `src/parameterization/stocks_and_demands.py`
- optionally `src/parameterization/nrcan_ceud.py`
- optionally `workflow/Snakefile` or a small rule file if workflow scaffolding exists
- `tests/` for focused smoke/unit tests

## Required behavior

- Load source and path configs.
- Resolve regions, year/version, URL template, table IDs, and table metadata from config.
- Create cache and interim directories if missing.
- Cache raw files under `inputs/cache/nrcan_ceud_transport/`.
- Use deterministic raw file names such as `{year}_tran_{region}_e_{table_id}.xls`.
- Reuse cached files on subsequent runs.
- Parse cached `.xls` files with behavior adapted from legacy:
  - skip metadata rows as needed;
  - clean row labels;
  - remove rows containing `Shares` or `GHG`;
  - convert year columns to integers;
  - convert values to floats;
  - treat `n.a.` as missing;
  - preserve table 36 multi-label behavior or document any deviation.
- Write normalized outputs under `inputs/interim/fetched_nrcan_ceud_inputs/`.
- Log failed/skipped tables with reasons.

## Preferred interim schema

At minimum, include:

- `source_id`
- `region`
- `table_id`
- `table_label`
- `short_name`
- `applies_to`
- `parameter_modules`
- `raw_series`
- `series_group`
- `series_name`
- `year`
- `value`
- `unit`
- `cached_file`

Use CSV unless Parquet support is already configured.

## Outputs

Expected output folder:
inputs/interim/fetched_nrcan_ceud_inputs/

Expected files:
- manifest.csv
- nrcan_ceud_transport_{region}.csv
- nrcan_ceud_transport_national.csv
- warnings.log                                  # or equivalent structured warning output

Do not force national extraction if the config is not ready. Log or document the gap.

## Tests / checks

Add small tests where practical:

* config loading;
* URL rendering;
* row-label cleaning;
* long-form normalization on a mocked dataframe.

Avoid tests requiring live network access.

## Acceptance criteria

* A documented command runs the extraction.
* Missing raw files are downloaded and cached.
* Existing raw files are reused.
* Interim files are produced in the expected folder.
* Rows preserve source/table/region/year/value provenance.
* Failures are logged.
* No legacy Excel workbook is copied, opened, or modified.
* No SQLite database is built.

## Progress

- [x] Inspected current scaffold, `config/sources.yaml`, and legacy `legacy_backend/transportation[deprecated]/get_nrcan_data.py`.
- [x] Registered deterministic raw CEUD cache file names under `inputs/cache/nrcan_ceud_transport/`.
- [x] Added a focused NRCan CEUD fetch/cache/normalize module with a CLI.
- [x] Added tests for URL rendering, cache path rendering, row-label cleaning, and mocked long-form normalization.
- [x] Ran an Ontario-only live extraction slice; first run downloaded 17 provincial tables, rerun reused cached files.
- [x] Ran tests and ruff.

## Outcomes

Commands run:

```powershell
uv run python -m parameterization.nrcan_ceud --scenario config/scenarios/legacy_reproduction.yaml --regions ON --skip-national
uv run pytest
uv run ruff check .
```

Generated outputs from the Ontario-only extraction slice:

- `inputs/cache/nrcan_ceud_transport/2021_tran_on_e_{table_id}.xls`
- `inputs/interim/fetched_nrcan_ceud_inputs/manifest.csv`
- `inputs/interim/fetched_nrcan_ceud_inputs/nrcan_ceud_transport_on.csv`
- `inputs/interim/fetched_nrcan_ceud_inputs/warnings.log`

Check results:

- `uv run pytest`: passed, 9 tests.
- `uv run ruff check .`: passed.

Known gaps:

- Only the Ontario provincial slice was generated during this milestone check; the CLI can include national tables and other configured provinces, but those were not downloaded in this run.
- The interim output remains source-normalized only. No parameter-ready derivations, legacy spreadsheet writes, or SQLite build were added.
