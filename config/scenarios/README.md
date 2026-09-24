# Scenario configuration

Scenario YAML files contain consequential run choices. Source availability, identity,
provenance, and status belong in `config/sources.yaml`; scenarios only select a registered
source year, edition, or trajectory when the run requires one. Shared unit factors belong in
`config/parameters/conversion.yaml`, and extraction or harmonization rules belong in
`config/parameters/rules.yaml`. #to-review

Use `legacy_reproduction.yaml` as the current authoring example. Its sections have these
roles:

- `scenario`, `geography`, and `periods` identify the run and define its temporal and
  regional scope.
- Model periods are labeled by their starting boundary, while parameter inputs reflect
  conditions at the end of the period. With a five-year step, the `2025` period uses
  projections for 2030.
- `sources.selections` contains run-specific selectors such as a source year or CER
  edition. All sources marked active in the registry are available to implemented ETL
  layers; a scenario does not maintain a second active-source list. #to-review
- `demand.cer_scenario` selects the CER trajectory for demand and its normalized source
  marker. `demand.future_car_demand` chooses GDP-indexed cars and passenger light trucks
  or a historical car CAGR extrapolation with light trucks receiving the remainder of their
  unchanged GDP-indexed combined passenger demand. The history window and method are owned by
  `config/parameters/rules.yaml`. #to-review
- `economics` is operational for the SQLite bootstrap and replaces the packaged v4
  global discount and default loan rates after the package defaults are checked.
  `cost_reference_currency` and `cost_reference_year` set the CER-harmonized
  currency year for transport investment and variable costs (currently 2020 CAD). #to-review
- `outputs` and `validation` control artifacts and legacy comparison; `switches`
  contains implemented modeling choices. The build includes all implemented parameter
  layers by default. A parameter omission selector will be added only if needed. #to-review
- `switches.survival_curves` chooses empirical road-vehicle survival curves
  instead of fixed median-equivalent lifetimes;
  `switches.survival_curve_max_age` bounds existing-stock cohorts when curves
  are enabled and bounds mileage profiles when `vkt_schedules` is enabled. #to-review
- `switches.vkt_schedules` selects age-dependent road utilization when true;
  false keeps CEUD-derived road utilization flat across model periods. #to-review
- `road_utilization.medium_truck_weight_source` selects national Wards class
  shares or Ontario Report 4 weight counts for the medium-truck ATB mileage
  aggregation. The Ontario choice is reserved for a reviewed Ontario-only
  mapping and currently fails explicitly; the national choice is operational. #to-review
- Optional `row_note_overrides.technology` replaces the note for a named structural
  technology during database creation. #to-review

Snakemake uses registered cached inputs without downloading by default. Pass
`--config download_sources=true` when refreshing sources; this is a workflow operation,
not a scenario modeling choice. #to-review

The configuration models reject unknown fields, inactive source selections, invalid
period grids, and rates outside zero to one. Quote province code `"ON"` because YAML 1.1
parsers may otherwise interpret `ON` as a boolean. #to-review
