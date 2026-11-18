# Diagnostic notebook instructions

Apply to notebooks in this directory alongside the repository `AGENTS.md`.
Use the scope map and affected `config/paths.yaml` routes to retrieve only the
relevant notebook, producer, helper, configuration and tests. Use the applicable
marimo skill for syntax and reactivity; this file defines diagnostic content policy.

## Notebook outline

1. **Question and scope:** identify the modeling choice, affected parameters,
   population, geography and years.
2. **Evidence and method:** identify inputs and the build/configuration where
   captured; explain filters, harmonization, units, weights, denominators and gaps.
3. **Diagnostic sections:** give each section one distinct insight using
   **question → evidence → interpretation → parameter consequence or unresolved
   limitation**. Place explanations beside their figures/tables; merge repetitions.
4. **Synthesis and limits:** state supported rationale, remaining uncertainty and
   possible improvements. Distinguish current methods, historical comparisons and
   exploratory alternatives.
5. **Audit details:** retain preparation commands, identities, coverage, provenance
   and full evidence in labelled expandable views or exports.

## Diagnostic boundaries

- Read prepared evidence. Opening a notebook must not fetch inputs, run ETL or
  publish production artifacts. Diagnostic calculations may examine alternatives;
  reuse backend owners for production methods rather than duplicating them.
- Preserve source and captured build/configuration identities. Missing, stale or
  incompatible evidence must give the owning preparation command; never repair a
  capture by substituting live inputs or configuration.
- Label observations, simulations, proxies and proposals. Distinguish missing
  evidence from zero, fitted closure from independent validation, and descriptive
  association from causal inference.
- Keep current-method views aligned with production. After accepted backend changes,
  refresh affected evidence, interpretations and documentation together under the
  root policy. Keep superseded claims out of the current-method narrative.

## Figures and tables

- Define the population, row/mark grain, units and denominator beside each view.
  Explain the finding, normalization and limits without relying on tooltips.
- Use readable labels, consistent category ordering and comparable scales.
  Distinguish counts/shares and source/derived values; keep overlapping comparison
  marks identifiable.
- Give each default table one question. Place comparable values and supporting
  counts together; verify estimator labels and avoid duplicate aliases. Preserve
  numeric sorting and round only for display.
- Defer raw keys, long provenance and implementation fields to accessible audit
  views. Preserve complete evidence while keeping the default view focused.

## Verification

- Reconcile displayed values with supporting evidence; check applicable keys,
  units, coverage, normalization, exclusions and fallback cases.
- Run focused tests for changed logic, `uv run ruff check <changed-python-paths>`,
  `uv run marimo check --strict <notebook.py>` and actual notebook execution.
- Inspect rendered default, dense and sparse/empty selections, including interactive
  tables, legends, overlap and clipping. Structural checks do not establish
  scientific correctness or usable rendering.
- Check complete cell output sizes. Send only required plot/tooltip fields and
  defer large audits without dropping observations or raising output limits.
  Keep snapshots and review captures local and out of Git.

## Scope map

Use this map for retrieval; update it when a notebook's responsibility changes.

| Notebook | Investigation and boundary |
| --- | --- |
| [vehicle_population_diagnosis.py](vehicle_population_diagnosis.py) | Mapping, unresolved identities, composition and retention; distinguish mapped support from full-fleet representativeness. |
| [ldv_efficiency_diagnosis.py](ldv_efficiency_diagnosis.py) | Ratings, empirical composition, ranges and the consumption-to-parameter bridge; label proxies and reference alternatives. |
| [stock_cohort_diagnosis.py](stock_cohort_diagnosis.py) | Acquisitions, stock calibration and cohort allocation; distinguish surviving stock from installed-equivalent capacity. |
| [vehicle_cost_diagnosis.py](vehicle_cost_diagnosis.py) | Source price bases, fleet/range and age aggregation, and conversion to model service costs. |
| [transportation_validation_diagnosis.py](transportation_validation_diagnosis.py) | Fleet, utilization, activity/energy, demand timing and charger checks; qualify what each comparison validates. |
| [ceud_calibration_diagnosis.py](ceud_calibration_diagnosis.py) | Energy calibration with activity preserved; fitted closure is not independent validation. |
| [travel_survey_diagnosis.py](travel_survey_diagnosis.py) | NHTS screening, travel and RAMP input derivation; separate reviewed derivation from weighted alternatives. |
| [ldv_charging_diagnosis.py](ldv_charging_diagnosis.py) | Production hourly normalization/rescaling and native RAMP charging, mobility and active drivers; preserve timing and annual utilization, and distinguish driving activity from charger occupancy. |
| [charger_infrastructure_diagnosis.py](charger_infrastructure_diagnosis.py) | Charger counts, power, costs and utilization; distinguish projections/alternatives from adopted assumptions. |
| [growth_constraint_diagnosis.py](growth_constraint_diagnosis.py) | Adoption evidence, deployment seeds and growth formulations; keep unpromoted candidates explicit. |
| [documentation_support.py](documentation_support.py) | Publication fleet/non-road figures; separate composition margins from a joint vehicle distribution. |
