# Scenario configuration

Scenario YAML files contain run choices. They select from the source registry in
`config/sources.yaml`; they do not repeat source URLs, citations, checksums, cache
templates, or data-quality evidence. Shared unit factors belong in
`config/parameters/conversion.yaml`, and extraction or harmonization rules belong in
`config/parameters/rules.yaml`.

Use `legacy_reproduction.yaml` as the current authoring example. Its sections have these
roles:

- `scenario`, `geography`, and `periods` identify the run and define its temporal and
  regional scope.
- `sources.active` enables registered sources. `sources.selections` contains only
  run-specific selectors such as a source year, CER edition, or CER scenario.
- `currency` defines the eventual harmonization target. The present fetchers retain
  source-native currency labels; parameter transformation will apply this section.
- `economics` is operational for the SQLite bootstrap and replaces the packaged v4
  global discount and default loan rates after the package defaults are checked.
- `outputs`, `validation`, and `switches` control artifacts and implemented execution.
- `row_note_overrides.technology` can replace the note for a named structural
  technology during database creation. `row_note_overrides.parameters` is reserved
  until parameter insertion is implemented and fails loudly if populated today.
- `planned` holds transport-relevant choices whose ETL behavior is not implemented yet.
  Keep these values `null`; a non-null value documents intent but does not activate a
  transformation.

The configuration models reject unknown fields, inactive source selections, invalid
period grids, rates outside zero to one, and negative validation tolerances. Quote
province code `"ON"` because YAML 1.1 parsers may otherwise interpret `ON` as a boolean.
