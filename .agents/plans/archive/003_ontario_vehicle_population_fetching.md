# 003_ontario_vehicle_population_fetching.md

## Goal

Add reproducible fetching and interim normalization for the Ontario Ministry of Transportation vehicle population data.

This source supports `stocks_and_demands.py` with:
- Report 5: vehicle age distributions for passenger, commercial, motorcycle, and bus classes.

And the planned `on_road_effs_and_costs.py` and `on_road_variable_costs.py` with:
- Report 4: commercial vehicle weight-class distribution for medium/heavy truck disaggregation.

Start with data year `2022`, because the legacy backend used that year. Do not build SQLite outputs yet.

## Context

The Ontario data catalogue resource URLs are not year-templated. Do not hardcode yearly ZIP URLs. Discover the correct ZIP through Ontario CKAN metadata for package `vehicle-population-data`, then cache it deterministically.

Reference legacy workflow is in legacy_backend/transportation[deprecated]/on_vehicle_population/, use ir for parser/tests.

Expected ZIP members vary by year:

- `{year}_Reg_Veh_Report4_Weight_Class&Status.TXT`
- `{year}_Reg_Veh_Report5_Class&Status&Descriptors.TXT`

## Files likely involved

- `config/sources.yaml`
- `config/paths.yaml` if path additions are needed
- `src/setup.py` or shared source-fetching utilities
- `src/utils.py`
- optionally `src/parameterization/fleet_attributes.py`
- optionally `src/parameterization/stocks_and_demands.py`
- optionally `src/parameterization/on_road_effs_and_costs.py`
- optionally `workflow/Snakefile` or small rule file if workflow scaffolding exists
- `tests/`

## Sources config expectations

Inspect `config/sources.yaml` first.

Keep the new entry consistent with existing source schema conventions. Avoid one-off keys. If new fields are needed for CKAN access, make them reusable and document them once near the top of `sources.yaml`.

Infer a clean YAML shape for CKAN package discovery. Likely reusable concepts include:

- access method / source access pattern;
- CKAN base URL;
- package ID;
- resource selector;
- cache path template;
- expected archive members;
- refresh/validation notes.

Add a short note that this CKAN access workflow should be generic enough for future Ontario Data Catalogue sources, not hardwired only to vehicle population data.

## Required behavior

- Load source metadata from `config/sources.yaml`.
- Resolve selected year, defaulting to 2022.
- Query CKAN package metadata for `vehicle-population-data`.
- Select the ZIP resource matching the requested year.
- Download/cache the ZIP under a deterministic path in `inputs/cache/`.
- Reuse cached ZIPs on rerun.
- Extract or read only Report 4 and Report 5.
- Write clean interim CSVs under `inputs/interim/fetched_ontario_vehicle_population/`.
- Log missing files, ambiguous resources, parsing issues, and skipped rows.

## Report 4 transformation

Use Report 4 to create commercial EPA GVWR distribution.

Required logic:

- keep `WEIGHT_CLASS == "COMMERCIAL"`;
- use `KG_FROM`, `KG_TO`, and `FIT-ACTIVE`;
- convert kg bounds to lb;
- classify rows using EPA GVWR bins;
- aggregate `FIT-ACTIVE` by EPA GVWR class;
- write both cleaned long/interim data and compact distribution output.

Legacy EPA labels:

```text
LDT1-2, LDT3-4, MDV2b, MDV3, MDV4, MDV5, MDV6, MDV7, MDV8
````

Legacy bin edges in lb:

```text
-inf, 6000, 8500, 10000, 14000, 16000, 19500, 26000, 33000, inf
```

## Report 5 transformation

Use Report 5 to create vehicle age distributions.

Required logic:

* keep `DESCRIPTOR == "YEAR"`;
* keep `VEHICLE_CLASS` in `PASSENGER`, `COMMERCIAL`, `MOTORCYCLE`, `BUS`;
* cast `VALUE` to integer model year;
* drop years newer than the data year;
* calculate `AGE = data_year - VALUE`;
* keep ages up to 30 unless configured otherwise;
* calculate `AGE_DIST = FIT-ACTIVE / class FIT-ACTIVE sum`;
* write cleaned and class-normalized age-distribution outputs.

## Expected outputs

Folder:

```text
inputs/interim/fetched_ontario_vehicle_population/
```

Files:

```text
manifest.csv
ontario_vehicle_population_report4_epa_gvwr_distribution_{year}.csv
ontario_vehicle_population_report5_age_distribution_{year}.csv
warnings.log
```

Include provenance columns where practical: source ID, year, report, raw file, cached ZIP, class, native count, and derived fields.

## Tests / checks

Add focused tests without live network access:

* CKAN resource selection from mocked package metadata;
* Report 4 parsing from the attached/example TXT schema;
* EPA GVWR bin assignment;
* Report 5 parsing from the attached/example TXT schema;
* age calculation and normalized age shares;
* cached ZIP reuse behavior where practical.

## Acceptance criteria

* `sources.yaml` has one consistent Ontario vehicle population entry.
* Any new CKAN-related fields are reusable and documented.
* A documented command can fetch/cache the 2022 ZIP via CKAN discovery.
* Required Report 4 and Report 5 files are found and parsed.
* Clean interim CSVs and derived distribution CSVs are written.
* Reruns reuse cached ZIPs.
* Tests pass.
* No SQLite database is built.
* No legacy Excel workbook or notebook is modified.

## Progress

- Completed: inspected `AGENTS.md`, this plan, `config/sources.yaml`, `config/paths.yaml`, `src/setup.py`, existing `src/parameterization/` patterns, `workflow/Snakefile`, tests, and `legacy_backend/transportation[deprecated]/on_vehicle_population/`.
- Completed: added reusable CKAN package metadata fields under `access:` in `config/sources.yaml`, documented once near the source-schema comments, and completed the Ontario vehicle population source entry with default year 2022.
- Completed: implemented `src/parameterization/ontario_vehicle_population.py` with CKAN package discovery, year-specific ZIP selection, deterministic cache reuse, Report 4 and Report 5 parsing, EPA GVWR distribution output, and class-normalized age distribution output.
- Completed: added a Snakemake rule target for the Ontario fetch/normalize step and documented the direct CLI plus Snakemake command in `README.md`.
- Completed: added focused mocked/local tests in `tests/test_ontario_vehicle_population.py`.
- Completed: ran the live 2022 fetch/normalize command. The first run discovered and cached `inputs/cache/ontario_vehicle_population/2022_vehicle_population_data.zip`; a rerun reused it.

## Outcomes

Files changed:

- `config/sources.yaml`
- `src/parameterization/ontario_vehicle_population.py`
- `tests/test_ontario_vehicle_population.py`
- `workflow/Snakefile`
- `README.md`
- `.agents/plans/003_ontario_vehicle_population_fetching.md`

Generated outputs:

- `inputs/cache/ontario_vehicle_population/2022_vehicle_population_data.zip`
- `inputs/interim/fetched_ontario_vehicle_population/manifest.csv`
- `inputs/interim/fetched_ontario_vehicle_population/ontario_vehicle_population_report4_cleaned_2022.csv`
- `inputs/interim/fetched_ontario_vehicle_population/ontario_vehicle_population_report4_epa_gvwr_distribution_2022.csv`
- `inputs/interim/fetched_ontario_vehicle_population/ontario_vehicle_population_report5_cleaned_2022.csv`
- `inputs/interim/fetched_ontario_vehicle_population/ontario_vehicle_population_report5_age_distribution_2022.csv`
- `inputs/interim/fetched_ontario_vehicle_population/warnings.log`

Commands run:

- `uv run pytest tests\test_ontario_vehicle_population.py` failed inside the sandbox because pytest could not access `C:\Users\rashi\AppData\Local\Temp\pytest-of-rashi`; rerun outside the sandbox passed with 7 tests.
- `uv run python -m parameterization.ontario_vehicle_population --scenario config/scenarios/legacy_reproduction.yaml --year 2022` initially found that the real ZIP stores reports under a `2022/` directory; member resolution was updated to allow exact or unique basename matches.
- `uv run python -m parameterization.ontario_vehicle_population --scenario config/scenarios/legacy_reproduction.yaml --year 2022` passed after that update and wrote interim outputs.
- `uv run pytest` passed: 18 tests.
- `uv run ruff check .` passed.

Known gaps:

- This does not build SQLite outputs or connect the interim files into downstream parameter insertion.
- CKAN selection is intentionally minimal: it filters by resource format and requested year across configured text fields. Future Ontario catalogue sources may need additional selector fields once their metadata patterns are observed.

## Configuration Boundary Update

Follow-up refactor completed after `AGENTS.md` clarified configuration boundaries:

- `config/paths.yaml` owns canonical roots such as `inputs.cache`, `inputs.interim`, and `config.parameters`.
- `config/sources.yaml` owns source identity, CKAN access metadata, source component metadata, URL templates, and cache filename templates. Cache templates are now relative to the configured cache root.
- `config/parameters/harmonization_rules.yaml` owns extraction/modeling assumptions that may change later, including interim subdirectories, manifest/warning/output filenames, CEUD Excel row offset, CEUD label/noise cleanup rules, Ontario Report 4 filters and EPA GVWR bins, Ontario Report 5 descriptor/class filters, and the default maximum age.
- `config/parameters/conversion_factors.yaml` owns the kg-to-lb conversion factor used for Ontario Report 4.
- Python modules keep implementation behavior: CKAN package querying, resource selection, cache reuse, ZIP member resolution, tabular parsing, dataframe transformations, and writing configured outputs.
- CEUD and Ontario Python modules now load these rules explicitly rather than carrying legacy-equivalent fallback assumptions in module constants.
- Ontario source components use `reports:` because Report 4 and Report 5 are source-native report files. Traceability stays consistent through shared component fields (`label`, `short_name`, `inputs`, `applies_to`, `produces`, and `parameter_modules`) rather than forcing every source family under `tables:`.

Additional files changed in this refactor:

- `config/paths.yaml`
- `config/parameters/harmonization_rules.yaml`
- `config/parameters/conversion_factors.yaml`
- `src/utils.py`
- `src/parameterization/nrcan_ceud.py`
- `src/parameterization/ontario_vehicle_population.py`
- `tests/test_nrcan_ceud.py`
- `tests/test_ontario_vehicle_population.py`

Additional commands run:

- `uv run pytest tests\test_nrcan_ceud.py tests\test_ontario_vehicle_population.py` hit the known sandbox temp-directory permission issue for two `tmp_path` tests; 13 focused tests passed before pytest fixture setup failed.
- `uv run python -c "from parameterization.utils import load_config_bundle, validate_config_bundle; b=load_config_bundle('config/scenarios/legacy_reproduction.yaml'); print(validate_config_bundle(b))"` passed with `[]`.
- `uv run pytest` passed outside the sandbox: 20 tests.
- `uv run ruff check .` passed.
- `uv run pytest` initially failed after removing Python fallback defaults because unquoted YAML `Unnamed: 0` parsed as a mapping; quoting that configured column name fixed the issue.
- Final `uv run pytest` passed outside the sandbox: 20 tests.
- Final `uv run ruff check .` passed.

## Test Runtime Update

The repeated sandbox failure was addressed by making pytest use repo-local runtime paths:

- `pyproject.toml` sets `--basetemp=.pytest-basetemp` so `tmp_path` fixtures do not use `C:\Users\rashi\AppData\Local\Temp`.
- `pyproject.toml` sets `cache_dir = ".pytest-cache-runtime"` so pytest cache writes do not depend on the previous `.pytest_cache` state.
- `.gitignore` excludes `.pytest-basetemp/` and `.pytest-cache-runtime/`.
- `uv run pytest tests\test_ontario_vehicle_population.py` now passes inside the sandbox: 8 tests.
- `uv run pytest` now passes inside the sandbox: 20 tests.
