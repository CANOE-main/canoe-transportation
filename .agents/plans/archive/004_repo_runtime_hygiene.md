# 004_repo_runtime_hygiene.md

## Goal

Make repo runtime and generated state predictable before the next ETL refactor. The hygiene pass should give contributors safe cleanup commands, a live-download-free doctor check, and clear verification tiers so stale pytest directories, Ruff caches, Snakemake state, generated outputs, and source caches do not block routine development.

## Context

- `AGENTS.md` defines numbered input roots: `inputs/0_cache/`, `inputs/0_external_models/`, `inputs/1_interim/`, and `inputs/2_processed/`.
- `config/paths.yaml` already exposes cache, external, interim, processed, output, schema, and legacy paths through config.
- `.gitignore` ignores virtualenv/runtime caches and numbered generated input roots; this plan should verify the classification before changing it further.
- `pyproject.toml` sets repo-local pytest runtime paths with `--basetemp=.pytest-basetemp` and `cache_dir=.pytest-cache-runtime`.
- `workflow/Snakefile` is still small and direct; there are no `workflow/rules/` files yet.
- Existing tests cover config loading and focused NRCan CEUD / Vehicle population parsing behavior.
- `README.md` includes user-edited input-parameter ETL flowcharts. Do not edit that section during this hygiene pass.

## Scope

1. Classify repo paths into source-controlled files, generated/runtime artifacts, cache/reference inputs, and legacy validation evidence.
2. Update `.gitignore` only where the classification proves a mismatch. Do not hide source files, manual inputs, schemas, configs, tests, or legacy validation evidence.
3. Add `scripts/clean_runtime.py` with dry-run default behavior.
4. Add a live-download-free repo doctor command or test that loads configs, resolves paths, checks generated directory creation, and verifies package imports.
5. Add concise verification-tier docs outside the README ETL flowchart section, or in a short developer note if that is cleaner.
6. Record any Snakemake startup/dry-run limitation as a follow-up instead of blocking Tier 0 / Tier 1 verification.

## Non-goals

- Do not implement new input-parameter ETL logic.
- Do not change modeling assumptions, source metadata semantics, units, or parameter mappings.
- Do not broadly rewrite the workflow architecture.
- Do not edit legacy artifacts.
- Do not edit the README input-parameter ETL flowcharts section.
- Do not debug a hanging Snakemake runtime beyond one minimal parse or dry-run attempt.

## Implementation steps

1. Inspect `AGENTS.md`, `.agents/PLANS.md`, `README.md`, `pyproject.toml`, `.gitignore`, `config/paths.yaml`, `config/sources.yaml`, `config/parameters/*.yaml`, `workflow/Snakefile`, `workflow/rules/*` if present, and tests.
2. Write a short runtime classification in the implementation notes or docs:
   - Source-controlled: Python packages, tests, configs, workflow files, schema, manual params, docs, plans.
   - Runtime/tool state: pytest cache/basetemp variants, Ruff cache, safe Snakemake metadata or locks, Python bytecode.
   - Generated backend outputs: `outputs/`, `inputs/1_interim/`, `inputs/2_processed/`.
   - Cache/exogenous inputs: `inputs/0_cache/`, `inputs/0_external_models/`, `inputs/0_manual_params/`.
   - Reference/parity evidence: `legacy_backend/`
3. Implement `scripts/clean_runtime.py`:
   - Default to `--dry-run`.
   - Clean tool/runtime state by default only.
   - Add `--include-generated` for generated outputs/interim/processed.
   - Add `--include-cache` for `inputs/0_cache/`; keep it explicit and opt-in.
   - Never clean `inputs/0_external_models/` in this pass.
   - Resolve every target path under the repo root before deletion and skip git-tracked files.
   - Never clean generated backend outputs by default; they support debugging and traceability during parity work.
4. Add a doctor entrypoint, preferably `scripts/doctor.py` or `python -m validation.repo_doctor`, that:
   - Loads YAML configs.
   - Resolves configured paths.
   - Verifies generated directories are creatable.
   - Verifies imports for `fetching`, `parameterization`, `utils.py`, and `validation`.
   - Does not fetch live data and does not require Snakemake.
5. Add focused tests for cleanup target selection and doctor behavior where practical.
6. Document verification tiers:
   - Tier 0: imports/config/doctor/Ruff/unit tests.
   - Tier 1: cache-only fetch and normalization smoke checks.
   - Tier 2: Snakemake dry-run.
   - Tier 3: baseline SQLite build and parity validation.

## Validation

Run and record:

```powershell
uv run python scripts/clean_runtime.py
uv run python scripts/doctor.py
uv run ruff check scripts src tests
uv run pytest
uv run python -m fetching.nrcan_ceud --scenario config/scenarios/legacy_reproduction.yaml --regions ON --skip-national --no-download
```

Attempt once and record pass/fail/hang without treating it as a blocker:

```powershell
uv run snakemake -n --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1
```

## Acceptance criteria

- A clean-runtime command exists and is dry-run safe by default.
- A doctor/smoke command exists and runs without live downloads.
- `.gitignore` matches the generated/runtime classification.
- Verification tiers are documented outside the README ETL flowchart content.
- Existing focused tests still pass.
- Ruff passes on touched files.
- Any Snakemake limitation is documented as a follow-up, not a blocker.
- Progress and outcomes record commands run and known limitations.
- Cleanup and doctor scripts are side-effect-light: `clean_runtime.py` defaults to dry-run, and `doctor.py` does not create, fetch, delete, or mutate anything except optionally creating missing generated directories when explicitly requested.

## Progress

- [x] Inspected current repo instructions, plan protocol, runtime config, source registry comments, parameter YAMLs, workflow file, and tests.
- [x] Classify runtime/generated/source-controlled paths in durable docs or implementation notes.
- [x] Implement safe cleanup script.
- [x] Implement live-download-free doctor command.
- [x] Update `.gitignore` only if the classification requires it.
- [x] Document verification tiers.
- [x] Run and record validation commands.

## Outcomes

Implemented `scripts/clean_runtime.py`, `scripts/doctor.py`, focused runtime hygiene tests, and README verification-tier notes.

Commands run:

- `uv run python scripts\doctor.py`: passed. It loaded configs, resolved paths, imported local packages, and did not mutate repo state.
- `uv run python scripts\clean_runtime.py`: passed as dry-run. It selected only runtime/tool state.
- `uv run python scripts\clean_runtime.py --include-generated --include-cache`: passed as dry-run. It selected `inputs/1_interim/`, `inputs/2_processed/`, and `inputs/0_cache/`, did not select `inputs/0_external_models/`, and skipped `outputs/` because it contains git-tracked files.
- `uv run ruff check scripts src tests\test_config.py tests\test_nrcan_ceud.py tests\test_vehicle_population.py tests\test_runtime_hygiene.py`: passed.
- `uv run pytest tests\test_config.py tests\test_nrcan_ceud.py tests\test_vehicle_population.py tests\test_runtime_hygiene.py --basetemp=.pytest-basetemp-runtime-pass -o cache_dir=.pytest-cache-runtime-pass`: passed, 24 tests.
- `uv run python -m fetching.nrcan_ceud --scenario config/scenarios/legacy_reproduction.yaml --regions ON --skip-national --no-download`: passed using cache-only inputs.
- `uv run pytest`: failed before affected tests ran because pytest could not remove the existing locked `.pytest-basetemp` directory and could not write `.pytest-cache-runtime`.
- `uv run snakemake -n --snakefile workflow\Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1`: hung without output; stopped after the single Tier 2 attempt.

Earlier known limitations before the workflow friction pass:

- Superseded: fixed pytest runtime directories previously blocked exact default `uv run pytest` startup on this Windows workspace. The workflow friction pass removed those fixed defaults and default `uv run pytest` now passes.
- Snakemake dry-run remains a Tier 2 follow-up issue because it hangs before output in this environment.

### Workflow Friction Reduction Update

Implemented a focused infrastructure stabilization pass to make Python modules the reliable development loop while keeping Snakemake as a minimal future orchestration smoke layer.

Changes:

- Removed fixed pytest runtime paths from `pyproject.toml`: no fixed `--basetemp=.pytest-basetemp` and no fixed `cache_dir=.pytest-cache-runtime`.
- Disabled pytest's cache provider by default and added `tests/conftest.py` to route test temporary files through a per-process repo-local temp root. This avoids both stale fixed repo temp directories and inaccessible Windows user temp roots.
- Reduced `workflow/Snakefile` to a minimal `outputs/logs/doctor.ok` smoke target that runs `uv run python scripts/doctor.py` and writes a small success marker.
- Removed top-level Snakefile config loading, YAML reads, fetch rules, and source-stage execution from the default workflow layer.
- Updated README verification tiers to state that Python modules are the current development loop and Snakemake is the future orchestration layer plus current workflow smoke check.
- Added runtime tests for the pytest config and minimal Snakefile contract.

Commands run:

- `uv run python scripts\doctor.py`: passed.
- `uv run python scripts\clean_runtime.py`: passed as dry-run.
- `uv run ruff check scripts src tests workflow`: passed.
- `uv run pytest tests\test_config.py tests\test_nrcan_ceud.py tests\test_vehicle_population.py tests\test_runtime_hygiene.py`: passed, 26 tests.
- `uv run pytest`: passed, 26 tests. This confirms default pytest no longer fails because of locked fixed `.pytest-basetemp` or `.pytest-cache-runtime`.
- `uv run snakemake -n --snakefile workflow\Snakefile --cores 1 outputs/logs/doctor.ok`: hung before output and was stopped.
- `uv run snakemake --snakefile workflow\Snakefile --cores 1 outputs/logs/doctor.ok`: started in parallel by mistake with the dry-run, also hung before output and was stopped. No Snakemake process remained afterward.

Known limitations:

- The Snakefile is now minimal and side-effect-free at parse time, but Snakemake still hangs before output in this Windows workspace. This appears to be outside the current Snakefile logic and remains a Tier 2 follow-up.
- `outputs/logs/doctor.ok` was not generated because the Snakemake smoke command did not reach rule execution.

## Decision log

- Use a separate cleanup script instead of ad hoc shell commands so path resolution and deletion behavior can be tested cross-platform.
- Keep cache deletion behind an explicit opt-in flag because cached upstream inputs are expensive to recreate and support offline Tier 1 checks.
- Treat Snakemake dry-run as Tier 2. If it hangs before output, record the limitation and continue with Tier 0 / Tier 1 validation.
