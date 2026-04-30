# 001_config_control_layer.md

## Goal

Create the first minimal YAML configuration layer for the CANOE transportation backend v2.0.

This should let the backend load canonical paths, source metadata, and a baseline scenario without hardcoded paths or scenario choices. This is a scaffold only: no full data downloading, parameter transformation, or SQLite compilation yet.

## Context

The v2.0 backend is replacing the legacy Excel/compiler workflow with a reproducible Snakemake + Python pipeline. The first milestone is baseline reproduction, so this config layer should support a legacy-equivalent baseline before experimental scenarios.

## Scope

Add or update:

- `config/paths.yaml`
- `config/sources.yaml`
- `config/scenarios/baseline.yaml`
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
4. Create `config/scenarios/baseline.yaml` with scenario name, region list, model years, active sources, output SQLite name, validation reference path, and baseline switches.
5. Add minimal Python helpers to load YAML, resolve paths, check required keys, and create configured directories.
6. Add or update `src/setup.py` so this command runs without network access:

   ```bash
   uv run python src/setup.py --scenario config/scenarios/baseline.yaml
   ```

7. Add a Snakemake smoke rule only if it fits the existing repo structure cleanly:

   ```bash
   uv run snakemake --snakefile workflow/Snakefile --config scenario=config/scenarios/baseline.yaml --cores 1
   ```

8. Add tests for YAML parsing, required keys, active scenario sources, and path resolution.

## Validation

The plan is complete when these pass or failures are documented:

```bash
uv run python src/setup.py --scenario config/scenarios/baseline.yaml
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

- [ ] Repo inspected.
- [ ] `config/paths.yaml` added or updated.
- [ ] `config/sources.yaml` added or updated.
- [ ] `config/scenarios/baseline.yaml` added or updated.
- [ ] YAML utilities added.
- [ ] Setup smoke validation added.
- [ ] Optional Snakemake smoke rule added.
- [ ] Tests added.
- [ ] Validation commands run.
- [ ] Outcomes summarized.

## Outcomes

Record final files changed, commands run, passing/failing checks, generated outputs, known placeholders, and recommended next plan.