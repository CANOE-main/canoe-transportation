# CANOE Transportation Backend

This repository contains the v2.0 refactor scaffold for the CANOE transportation backend.

The current implementation is an early configuration-control layer. It loads YAML configuration, validates required scaffold keys, creates configured working directories, and writes a setup smoke status artifact. It does not compile SQLite databases yet.

## Interim Source Fetching

Fetch/cache and normalize the baseline Ontario vehicle population Reports 4 and 5:

```powershell
uv run python -m parameterization.ontario_vehicle_population --scenario config/scenarios/legacy_reproduction.yaml --year 2022
```

The same step can be run through Snakemake by targeting the year-specific interim outputs, for example:

```powershell
uv run snakemake --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1 inputs/interim/fetched_ontario_vehicle_population/ontario_vehicle_population_report5_age_distribution_2022.csv
```
