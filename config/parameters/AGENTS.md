# Parameter contract instructions

Apply alongside the [repository instructions](../../AGENTS.md) when changing parameter
configuration, reviewed configuration CSVs or their loading/provenance/capture consumers.
Start with [rules.yaml](rules.yaml) to locate the executable owner. Use the map below
and [paths.yaml](../paths.yaml) to retrieve only the affected contract, producer,
parameter builder, insertion boundary and tests.

## Context routing

| Rule partition | What it controls |
| --- | --- |
| [source_normalization.yaml](source_normalization.yaml) | Source layouts, columns, selectors, filters, normalization and interim audit files for the fetching adapters. |
| [fleet_stock_and_activity.yaml](fleet_stock_and_activity.yaml) | Reviewed manual-table selectors, vehicle mappings and aggregation, stocks, demand, utilization and lifetimes. |
| [vehicle_efficiency_and_emissions.yaml](vehicle_efficiency_and_emissions.yaml) | Fuel/electricity consumption, PHEV treatment, optional CEUD energy calibration and vehicle-cycle emissions. |
| [transport_costs.yaml](transport_costs.yaml) | Investment, maintenance and operating-cost conventions. |
| [charging_and_range_representation.yaml](charging_and_range_representation.yaml) | Aggregate/explicit charger infrastructure, hourly charging and LDV electric-range representation. |
| [transport_diagnostic_controls.yaml](transport_diagnostic_controls.yaml) | Opt-in comparison references, notebook preparation and shared diagnostic presentation controls. These do not become production modeling assumptions. |

Read a specialized contract only when its schema or consumer is affected:

| File | What it controls |
| --- | --- |
| [conversion.yaml](conversion.yaml) | Shared physical, service and fuel-energy unit conversions. |
| [dunsky_charger_inputs.yaml](dunsky_charger_inputs.yaml) | Dunsky report/table validation and source-native extraction maps. Production interpretation lives in the charging partition. |
| [travel_survey.yaml](travel_survey.yaml) | Travel-survey normalization and comparison with registered RAMP evidence. |
| [ldv_charging_diagnostic.yaml](ldv_charging_diagnostic.yaml) | Registered hourly charging runs and their read-only diagnostic views. |
| [charger_infrastructure_diagnosis.yaml](charger_infrastructure_diagnosis.yaml) | Charger evidence views and unadopted stock/recharge/utilization sensitivities. |
| [technology_growth_diagnostic.yaml](technology_growth_diagnostic.yaml) | Opt-in technology-adoption evidence, fitting and review controls. |
| [documentation.yaml](documentation.yaml) | Publication figure, fleet-table and nonroad-table selections. |

## Rule-setting boundaries

- Keep extraction, harmonization and accepted parameter methods in their owning rule
  partition. Put source identity, access, native metadata, citations and DQ in
  [sources.yaml](../sources.yaml), run choices in [scenario YAML](../scenarios/README.md),
  and artifact locations in [paths.yaml](../paths.yaml).
- Keep reusable physical conversions in `conversion.yaml`, source-specific report
  extraction in its adapter contract, and presentation/research sensitivities in their
  diagnostic contracts. Promote an accepted method in its production owner and reuse
  it in diagnostics; notebook calculations do not establish production adoption.
- Treat [vehicle_reference.csv](vehicle_reference.csv) and
  [vehicle_source_links.csv](vehicle_source_links.csv) as reviewed authorities for
  vehicle facts and source links. Keep reviewed observations/assumptions in their
  owning tables; YAML selects and transforms them without duplicating their values.

## Parameter lineage and changes

1. Identify the affected parameter, selected scenario branch, geography, years, row
   grain, keys and units before editing a rule.
2. Trace registered source/component or internal dataset → registered input/reviewed
   table → normalization rule and adapter → interim evidence → parameter owner →
   final validated row. Check conversions, currency/source/model years, weights and
   denominators, coverage, exclusions/fallbacks and the full provenance/DQ chain.
3. Modify the existing owner. Keep a coherent contract together; add a partition only
   for a distinct, recurring review boundary. Declare ownership in the index and update
   this map. Use flat, descriptive lowercase filenames. No glob discovery, recursive
   includes, cross-file overrides or deep merging are supported.
4. For structural moves, preserve every owner payload, scalar/key type, nested order,
   selection, default and parameter consequence. Separate intentional scientific
   changes and their evidence from configuration maintenance.

## Loading, freshness and verification

- Keep the version-3 routing index free of parameter values and declare each owner
  exactly once in version-1 members. Use the [shared loader APIs](../../src/utils/__init__.py):
  `load_harmonization_rules` for a bundle owner, `load_rules_for_owner` for scoped
  bootstrap/presentation reads, and `load_rules_config` for complete validation.
- Use `harmonization_rule_paths` / `harmonization_rule_fingerprints` for the entire
  declared dependency set. Copy and verify every member in tests/captures. Bind the
  verified captured parameter directory before validation; never substitute live files
  or rewrite stored hashes. Historical version-2 captures stay in their recorded roots.
- Treat comment/layout edits as byte-fingerprint changes. Before presenting affected
  reports or captures as current, regenerate them through their owning producer.
  Source-update adoption requires a current schema-3 review of the complete rules set.
- Run checks from the repository root. For routing/structure changes use
  `uv run --offline pytest tests/test_parameter_rules.py tests/test_config.py -q`.
  Add the affected parsing, conversion, coverage, provenance and insertion checks;
  exercise a real offline build/capture when runtime behavior or identity handling changes.
  Follow root isolation rules and the [notebook policy](../../docs/insights/AGENTS.md)
  when diagnostic evidence is affected.
