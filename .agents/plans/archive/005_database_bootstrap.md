# 005 Database Bootstrap

## Goal

Create a scenario-configured Temoa SQLite database, load the current technology and
commodity templates, run basic SQLite/schema/data checks, and write a concise JSON
validation artifact through one direct Python command.

## Context

- The worktree already replaces the deleted `inputs/canoe_dataset_schema.sql` with
  `inputs/temoa_schema_v4.sql`; `config/paths.yaml` still points at the deleted path.
- The current schema defines lowercase `technology` and `commodity` tables and seeds
  their `technology_type` and `commodity_type` foreign-key lookup values.
- `technology.csv` currently has 134 records and one schema-incompatible field,
  `notes`; `commodity.csv` currently has 51 records and maps directly. These counts
  are observations only and must not be encoded in implementation or tests.
- Blank CSV cells in schema columns with defaults should use those schema defaults;
  other blanks can be inserted as SQLite NULL values.
- `technology.csv` is now UTF-8. An earlier focused run encountered its prior
  Windows-1252 encoding, but the template was subsequently normalized without
  changing its source text.
- `region.csv` and `time_period.csv` are available starting templates but are outside
  this bounded technology/commodity load.

## Scope

1. Point the canonical configured schema path at the supplied replacement schema and
   add a configured template-directory path.
2. Replace the `src/build_transport.py` placeholder with a direct scenario entrypoint.
3. Derive CSV columns and insert mappings from CSV headers and SQLite table metadata.
4. Report ignored fields, missing optional fields, schema-default use, and failures.
5. Check table presence, inserted primary-key coverage, row counts, foreign keys, and
   SQLite integrity before publishing the database.
6. Protect an existing target unless the caller explicitly passes `--overwrite`.
7. Write the configured JSON validation report and add focused tests.

## Non-goals

- Do not load region, time-period, or other model/parameter tables.
- Do not fetch sources, transform parameters, compare parity, or edit legacy inputs.
- Do not modify Snakemake or the README ETL flowcharts.
- Do not generalize this into a source-agnostic ETL framework.

## Implementation steps

1. Update config paths and config tests for the replacement schema/template root.
2. Implement a small table specification for technology and commodity templates.
3. Read each CSV with the standard library, inspect target columns with
   `PRAGMA table_info`, reject missing required columns, and insert only compatible
   fields in one transaction.
4. Build in a temporary sibling file and publish atomically only after validation.
5. Add validation helpers under `src/validation/` and emit the scenario-configured
   JSON report.
6. Test creation/insertion/integrity, mismatch reporting, and overwrite protection.

## Validation

Run and record:

```powershell
uv run python scripts/doctor.py
uv run ruff check scripts src tests
uv run pytest
uv run python src/build_transport.py --scenario config/scenarios/legacy_reproduction.yaml
```

Then independently query the configured database to confirm current technology and
commodity counts and run `PRAGMA integrity_check` and `PRAGMA foreign_key_check`.

## Acceptance criteria

- The direct command reproducibly creates the configured database and report.
- Current technology and commodity records are present without hard-coded row counts
  or record identifiers.
- CSV/schema mismatches are explicit in the artifact or fail with row/file context.
- Existing database targets are unchanged without explicit overwrite authorization.
- Focused tests, the full test suite, Ruff, and the repo doctor pass.

## Progress

- [x] Inspected repository instructions, prior plan, config/runtime utilities, schema,
  all four starting templates, build placeholder, README, and existing tests.
- [x] Recorded the pre-existing schema replacement and current table-contract findings.
- [x] Updated configuration and implemented the bootstrap command.
- [x] Added focused tests for creation/insertion/integrity, mismatch reporting, and
  overwrite protection.
- [x] Ran and recorded all validation commands and generated artifacts.

## Outcomes

Implemented a direct `src/build_transport.py` entrypoint that resolves the schema,
template directory, SQLite output, and validation artifact through the config bundle.
It builds in a temporary sibling file, maps current CSV headers against live SQLite
metadata, uses schema defaults for blank defaulted fields, validates before atomic
publish, and requires `--overwrite` to replace an existing database.

Generated artifacts:

- `outputs/sqlite/canoe_transport_legacy_reproduction.sqlite`
- `outputs/validation/legacy_reproduction_database_bootstrap.json`

Observed results are derived from the current inputs: 134 technology records and 51
commodity records were inserted; all source primary keys matched, `integrity_check`
returned `ok`, and `foreign_key_check` returned no violations. The report records the
ignored technology `notes` field, schema-default/NULL handling, and selected encodings.
After the template's subsequent UTF-8 normalization, a fresh bootstrap report will
record UTF-8 for `technology.csv`; the originally generated artifact reflects the
template state at the time of that validation run.

Commands run:

- `uv run python scripts/doctor.py`: passed (`ok=True`, no mutation).
- `uv run ruff check scripts src tests`: passed.
- `uv run pytest`: passed, 30 tests.
- `uv run python src/build_transport.py --scenario config/scenarios/legacy_reproduction.yaml`:
  passed on a missing target and created both configured artifacts.
- The same command run again without `--overwrite`: refused the existing database as
  intended and left it unchanged.
- `uv run python src/build_transport.py --scenario config/scenarios/legacy_reproduction.yaml --overwrite`:
  passed when explicitly refreshing the database generated by this task.
- Independent SQLite queries confirmed the current 134/51 row counts,
  `PRAGMA integrity_check = ok`, and an empty `PRAGMA foreign_key_check` result.

Known limitations: only technology and commodity templates are loaded; no parity claim
is made, and the source technology `notes` field remains intentionally reported rather
than added to the supplied schema. The recommended next bounded slice is to load and
validate the existing region and time-period templates, including scenario-region/year
coverage checks, before any parameter tables are introduced.

## Decision log

- Use the supplied `temoa_schema_v4.sql` rather than restoring the deleted older
  schema. This follows the current worktree evidence and makes the configured schema
  path truthful.
- Keep the bootstrap table list explicit (`technology`, `commodity`) while deriving
  their columns, defaults, primary keys, and row counts at runtime. This is the
  smallest extensible boundary for later template-table slices.
- Read templates as strict UTF-8 first and record the selected encoding per table.
  The Windows-1252 compatibility fallback remains defensive support for older template
  snapshots, but the current `technology.csv` no longer requires it.
