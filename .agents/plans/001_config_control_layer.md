# 001_config_control_layer.md

## Goal

Create the first minimal YAML configuration layer for the CANOE transportation backend v2.0.

This should let the backend load canonical paths, source metadata, and a legacy reproduction scenario without hardcoded paths or scenario choices. This is a scaffold only: no full data downloading, parameter transformation, or SQLite compilation yet.

## Context

The v2.0 backend is replacing the legacy Excel/compiler workflow with a reproducible Snakemake + Python pipeline. The first milestone is legacy reproduction, so this config layer should support reproducing legacy-equivalent behavior before experimental scenarios.

## Current repository findings

- `src/` exists and should follow the planned architecture from `AGENTS.md`: thin entry-point scripts at `src/setup.py` and `src/build_transport.py`, with shared implementation under `src/parameterization/`.
- `config/`, `workflow/`, and `tests/` do not exist yet.
- `inputs/` currently contains only `inputs/canoe_dataset_schema.sql`.
- Legacy validation evidence is under `legacy_backend/`, including `legacy_backend/canoe_on_12d_baseline.sqlite`, `legacy_backend/canoe_schema.sql`, deprecated compiler scripts, spreadsheet inputs, constraint workbooks, RAMP-mobility outputs, GREET files, and analysis CSVs/notebooks.
- `pyproject.toml` includes `pyyaml`, `snakemake`, `pytest`, and `ruff`; uv build packaging is explicitly configured to use the `parameterization` module from the planned scaffold.

## Scope

Add or update:

- `config/paths.yaml`
- `config/sources.yaml`
- `config/scenarios/legacy_reproduction.yaml`
- minimal YAML-loading utilities
- setup smoke validation
- optional Snakemake smoke rule
- basic tests for config loading and required keys

## Non-goals

Do not implement NRCan downloads, Excel extraction, parameter transformation, SQLite compilation, scenario inheritance, or advanced source validation in this plan.

Do not invent source data, formulas, or final table mappings. Use clear placeholders where details are uncertain.

## Implementation steps

1. Inspect the repository structure and existing config/workflow/test files.
2. Create `config/paths.yaml` with canonical input, cache, external, interim, processed, output, SQLite, validation, log, schema, and legacy-reference paths.
3. Create `config/sources.yaml` with initial entries for NRCan CEUD transport, RAMP-mobility LDV charging profiles, and placeholders for AEO/GREET-style sources.
4. Create `config/scenarios/legacy_reproduction.yaml` with scenario name, region list, model years, active sources, output SQLite name, validation reference path, and legacy reproduction switches.
5. Add minimal Python helpers to load YAML, resolve paths, check required keys, and create configured directories.
6. Add or update `src/setup.py` so this command runs without network access:

   ```bash
   uv run python src/setup.py --scenario config/scenarios/legacy_reproduction.yaml
   ```

7. Add a Snakemake smoke rule only if it fits the existing repo structure cleanly:

   ```bash
   uv run snakemake --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1
   ```

8. Add tests for YAML parsing, required keys, active scenario sources, and path resolution.

## Validation

The plan is complete when these pass or failures are documented:

```bash
uv run python src/setup.py --scenario config/scenarios/legacy_reproduction.yaml
uv run pytest
uv run ruff check .
```

Run the Snakemake command too if a smoke rule is added.

## Acceptance criteria

- YAML configs load successfully.
- Active scenario sources are defined in `sources.yaml`.
- Configured directories can be resolved and created.
- Setup smoke validation writes a small status/log artifact.
- Tests do not require real upstream downloads.
- Uncertain source details are marked as placeholders.
- No modeling assumptions are changed.

## Progress

- [x] Repo inspected.
- [x] `config/paths.yaml` added or updated.
- [x] `config/sources.yaml` added or updated.
- [x] `config/scenarios/legacy_reproduction.yaml` added or updated.
- [x] YAML utilities added.
- [x] Setup smoke validation added.
- [x] Optional Snakemake smoke rule added.
- [x] Tests added.
- [x] Validation commands run.
- [x] Outcomes summarized.

## Outcomes

Files changed:

- Added `README.md`.
- Added `config/paths.yaml`, `config/sources.yaml`, and `config/scenarios/legacy_reproduction.yaml`.
- Added `src/setup.py`, `src/build_transport.py`, `src/parameterization/__init__.py`, `src/utils.py`, `src/validation/__init__.py`, and `src/validation/config_smoke.py`.
- Added `workflow/Snakefile`.
- Added `tests/test_config.py`.
- Updated `pyproject.toml` to configure uv build packaging for `parameterization` and `validation`, add `src` to pytest import paths, and exclude `legacy_backend`, `.snakemake`, and `outputs` from Ruff.
- `uv.lock` was refreshed by `uv run` to match the current project metadata.

Commands run:

- `uv run python src/setup.py --scenario config/scenarios/legacy_reproduction.yaml`: passed.
- `uv run pytest`: passed, 5 tests.
- `uv run ruff check .`: passed.
- `uv build`: passed; built an sdist and wheel using the explicit `parameterization` module configuration.
- `uv run snakemake --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1`: passed after correcting the Snakefile config path to `config/paths.yaml`.

Generated outputs:

- `outputs/logs/setup_smoke_legacy_reproduction.json`, with `ok: true`, existing schema and legacy reference checks, created working directories, active sources, placeholder source list, and non-goals set to false for downloads, transformations, and SQLite compilation.
- Created working directories under `inputs/cache`, `inputs/external`, `inputs/interim`, `inputs/processed`, `outputs/sqlite`, `outputs/validation`, and `outputs/logs`.

Known placeholders:

- `sources.yaml` marks NRCan CEUD transport and RAMP-mobility LDV charging as active placeholder sources.
- AEO-style and GREET-style sources are registered as inactive placeholders.
- Exact URLs, citations, source versions, checksums, units, and extraction maps remain unresolved.

Recommended next plan:

- `002_sqlite_schema_instantiation.md`: instantiate an empty target SQLite database from `inputs/canoe_dataset_schema.sql`, validate schema tables/keys, and keep it separate from data extraction or parity comparison.
