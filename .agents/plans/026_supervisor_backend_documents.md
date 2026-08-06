# Supervisor-facing backend documents

## Goal

Create two concise, evidence-based documents for supervisor discussion: an executive
summary of transportation backend v2.0 and a human-facing inventory of registered
transportation sources. Preserve the distinction between implemented infrastructure,
diagnostic/WIP work, and planned parameter/scenario capabilities.

## Context

Evidence inspected before documentation edits:

- Repository policy and planning: `AGENTS.md`, `.agents/PLANS.md`, and the initial
  working-tree state. The worktree contains extensive unrelated modified/untracked work;
  this slice will add only this plan and the two requested documents.
- Architecture and lineage: `docs/backend_architecture.md` plus only the overview,
  legend, and shared manual-selector portion of `docs/etl_flowcharts.md`.
- Configuration: `config/paths.yaml`, the complete parsed registry in
  `config/sources.yaml`, ownership comments and implemented sections of
  `config/parameters/rules.yaml`, all of `conversion.yaml`, and
  `config/scenarios/README.md` plus `legacy_reproduction.yaml`.
- Orchestration and code: `workflow/Snakefile`; module responsibilities and relevant
  entrypoints in `src/fetching/`, `src/parameterization/`, and `src/validation/`;
  `src/build_transport.py`; and focused tests at those interfaces.
- Current artifacts: cache, external-model, manual, interim, SQLite, log, and validation
  directories; source manifests; manual-parameter reconciliation/findings; the current
  SQLite table counts and foreign-key check; and measured artifact sizes.
- Diagnostics: `docs/insights/vehicle_population_aggregation_mapping.py` and its
  exported HTML validation artifact.
- Completed evidence records used only to corroborate implementation/validation:
  ExecPlans 007, 009, 015, 020, and 025. Current code, config, and artifacts take
  precedence over their historical statements.

Current findings that constrain the documents:

- YAML ownership is explicit: paths in `paths.yaml`; source identity/access/provenance
  in `sources.yaml`; extraction and harmonization in `rules.yaml`; reusable factors in
  `conversion.yaml`; run selections and modelling switches in scenario YAML.
- Implemented fetchers publish source-native cached/interim artifacts, manifests, and
  warnings with offline modes. Existing manifests cover provincial CEUD, Ontario MTO,
  StatCan, CER, NLR/ANL, NRCan Ratings, FuelEconomy.gov, and assorted public sources.
  A national CEUD normalized manifest is not present.
- Review-owned manual CSVs are registered and expanded to technology selectors with
  reconciliation and explicit unresolved findings; unresolved rows are retained.
- Current parameterization modules produce diagnostic/audit artifacts for manual
  selectors, road aggregation, Ontario existing-stock cohorts, and lifetime/survival
  evidence. `inputs/2_processed/` has no current files.
- The database builder uses pinned `canoe-schema` v4 models/DDL, internal provenance for
  backend-owned templates, integrity/foreign-key/provenance audits, atomic publication,
  and a limited technology/commodity legacy comparison. The current SQLite artifact is
  a structural bootstrap, not a full parameterized parity build; the scenario sets
  `transform_parameters: false` and keeps future modelling choices under `planned`.
- The compact Snakefile currently orchestrates doctor, StatCan, CER, and structural
  database stages, not every implemented adapter or parameter artifact.
- `docs/insights/` contains one substantial vehicle mapping/survival diagnostic notebook
  and an HTML export. Diagnostic coverage for the wider backend remains WIP.
- All 21 registry entries currently carry the literal registry status `pending`, even
  where code, cached inputs, manifests, tests, and interim outputs demonstrate an
  implemented adapter. The inventory will state evidence-based lifecycle status and
  disclose this registry-status gap rather than silently equating `pending` with absence.

## Scope

- Add `docs/transport_backend_executive_summary.md` at approximately 700--1,000 words.
- Add `docs/source_inventory.md` with one primary table covering all registered source
  families, consolidated within families, and coarse measured size categories.
- Use explicit **Implemented**, **In progress**, **Planned**, and **Decision needed**
  labels where lifecycle state matters.
- Include one simple, syntactically valid 8--10-node Mermaid architecture overview.
- Record validation results, decisions, and unresolved gaps here.

## Non-goals

- No implementation, configuration, README, source artifact, notebook, workflow, or
  generated database changes.
- No detailed equations, source-table listings, mappings, or parameter-specific ETL
  duplication.
- No claim that the current structural database is a parameterized legacy-parity build.
- No source refresh, network access, or reinterpretation of unresolved evidence.

## Implementation steps

- [x] Inspect the required policy, architecture, config, code, artifacts, notebooks,
  focused tests, and useful completed-plan outcomes.
- [x] Draft the executive summary around verified implemented/current/planned boundaries.
- [x] Draft the source inventory from the authoritative registry and measured artifacts.
- [x] Validate Markdown paths/links, Mermaid syntax, word count, table coverage, status
  labels, and the restricted diff.
- [x] Record final commands, outcomes, deviations, and unresolved gaps.

## Validation

Planned static/interface checks:

- Parse `config/sources.yaml` and confirm every registered source key maps to exactly one
  inventory family (allowing the two CEUD keys to consolidate into one family row).
- Resolve every local Markdown link target from each document's directory.
- Extract the Mermaid block and parse it with the repository's installed Mermaid tooling
  if available; otherwise run a focused structural validator and record that limitation.
- Count executive-summary prose words and Mermaid conceptual nodes.
- Run focused config, provenance, database-bootstrap, legacy-comparison, source-adapter,
  and Marimo checks only as needed to validate claims; documentation does not justify a
  full source refresh or end-to-end parameter build.
- Run `git diff --check` and a path-restricted diff/status review for this plan and the
  two documents.

## Acceptance criteria

- Both documents are concise, human-facing, and traceable to current repository evidence.
- The executive summary is approximately 700--1,000 words with no more than five major
  remaining priorities and a roughly 8--10-node Mermaid overview.
- The inventory covers upstream downloads/APIs, manual downloads, registered external
  model inputs, and reviewed manual assumptions, and distinguishes internal templates.
- Approximate sizes use measured artifacts or `Not yet measured`, never guesses.
- Implemented, in-progress, planned, and decision-needed work are not conflated.
- Links and Mermaid validate; validation results and remaining gaps are recorded below.
- Only this plan and the two requested documents are changed by this slice, ready for one
  logical documentation commit.

## Progress

- [x] Research and boundary definition complete.
- [x] Documentation drafting complete.
- [x] Validation complete.
- [x] Documentation commit complete.

## Outcomes

Documentation decisions:

- The executive summary treats the source-to-SQLite diagram as the intended control and
  artifact flow, while its prose states that processed parameter inputs and full
  parameter insertion are not yet complete.
- Inventory lifecycle labels are based on current code/manifests/artifacts and are
  explicitly separated from both scenario activation and the registry's literal
  all-`pending` status field.
- The two CEUD entries are one source family; every other registry entry has its own
  family row. Backend templates are documented outside the table because they are
  internal structural inputs, not external sources.
- Only one current Marimo notebook is described, and wider diagnostic coverage is WIP.
  Planned scenario fields are not described as implemented transformations.

Validation performed on 2026-08-06:

- A temporary local validator (removed after use) resolved all 14 Markdown links,
  counted 728 executive-summary prose words, 10 unique Mermaid nodes, five remaining
  priorities, and 20 inventory rows covering all 21 source keys with only the intended
  two-to-one CEUD consolidation.
- Mermaid CLI `mmdc` is not installed. The simple `flowchart LR` block received focused
  structural review for one declaration per node, supported node shapes, labelled
  solid/dashed edges, balanced delimiters, and a valid fenced block; no renderer result
  is claimed.
- `uv run pytest tests/test_config.py tests/test_database_bootstrap.py
  tests/test_provenance.py tests/test_legacy_compare.py tests/test_statcan_tables.py
  tests/test_cer_enerfuture.py tests/test_nlr_atb_autonomie.py tests/test_nrcan_ceud.py
  tests/test_vehicle_population.py tests/fetching/test_assorted_sources.py
  tests/test_fueleconomy_vehicles.py tests/test_manual_parameters.py -q` produced
  134 passes and one failure. The failure is a current-tree evidence mismatch: the
  database-bootstrap test expects only T01--T20, while `sources.yaml` now registers
  FuelEconomy.gov as T21. This documentation-only task does not alter that unrelated
  modified test or the registry.
- `uv run marimo check docs/insights/vehicle_population_aggregation_mapping.py` passed.
- Read-only evidence checks confirmed 91 tables in the current structural SQLite, no
  `foreign_key_check` rows, an empty `inputs/2_processed/`, current source manifests,
  manual reconciliation/findings, and the coarse artifact-size categories used in the
  inventory.
- No source refresh, network access, full parameter build, generated artifact rewrite,
  implementation/configuration/README edit, or interpretation of unresolved source
  evidence was performed.

Unresolved gaps carried forward:

- The national CEUD cache/normalized manifest is absent.
- The registry status lifecycle needs a project definition and reconciliation with
  implemented adapters.
- The T21 exact-mapping test assertion is stale relative to the registry.
- Full parameter construction, processed artifacts, parameter-table parity, and broad
  notebook coverage remain incomplete as described in the executive summary.
- Path-restricted staged review contains exactly this plan and the two requested
  documents. `git diff --cached --check` passed with only Windows line-ending notices;
  the three files are being published as one logical documentation commit.
