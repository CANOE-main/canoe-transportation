# CANOE Transportation Backend

This repository is the v2.0 refactor of the CANOE transportation backend. It is building
a reproducible Python and Snakemake pipeline that turns registered transport data into
validated CANOE/Temoa SQLite inputs.

The project is still maturing. Configuration loading, source adapters, normalized
interim artifacts, provenance, schema-backed insertion, atomic database publication,
and focused validation are implemented. The full transport parameterization and legacy
SQLite parity are not yet complete; the current database output is a validated
technology/commodity bootstrap.

## Backend architecture

```text
YAML configuration
    -> source adapters and cached inputs
    -> normalized interim tables
    -> parameter modules (incremental)
    -> canoe-schema v4 SQLite build
    -> integrity and legacy-parity reports
```

The current repository layout provides the following navigation:

```text
.
├── AGENTS.md                       # Stable repository policy
├── README.md                       # Human-facing project orientation
├── .agents/
│   ├── PLANS.md                    # ExecPlan protocol
│   ├── plans/                      # Task-local implementation records
│   └── skills/                     # Optional task-retrieved procedures
├── config/
│   ├── paths.yaml                  # Canonical directories and artifact paths
│   ├── sources.yaml                # External-source registry and provenance
│   ├── scenarios/
│   │   ├── README.md               # Scenario authoring contract
│   │   └── legacy_reproduction.yaml
│   └── parameters/
│       ├── rules.yaml              # Extraction and harmonization contracts
│       └── conversion.yaml         # Reusable conversion factors
├── workflow/
│   └── Snakefile                   # Dependency and artifact orchestration
├── src/
│   ├── setup.py                    # Config smoke validation and setup status
│   ├── build_transport.py          # Validated SQLite build entrypoint
│   ├── fetching/
│   │   ├── nrcan_ceud.py           # NRCan CEUD tables
│   │   ├── vehicle_population.py   # Ontario vehicle population reports
│   │   ├── statcan_tables.py       # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py       # CER energy-future tables
│   │   └── nlr_atb_autonomie.py    # NLR ATB and ANL Autonomie inputs
│   ├── parameterization/           # Parameter modules added incrementally
│   ├── utils/                      # Typed config and path utilities
│   └── validation/
│       ├── config_models.py        # Pydantic configuration contracts
│       ├── provenance.py           # Source and dataset provenance
│       ├── schema_contract.py      # canoe-schema v4 compatibility
│       ├── insertion.py            # Validated parameterized insertion
│       ├── database_bootstrap.py   # Integrity and publication checks
│       └── legacy_compare.py       # Legacy SQLite comparison
├── scripts/
│   ├── doctor.py                   # Non-mutating repository readiness check
│   └── clean_runtime.py            # Explicit runtime cleanup
├── inputs/
│   ├── 0_canoe_template/           # Backend-owned structural templates
│   ├── 0_cache/                    # Authoritative cached downloads
│   ├── 0_external_models/          # Registered external-model artifacts
│   ├── 1_interim/                  # Normalized auditable tables
│   └── 2_processed/                # Parameter-ready tables
├── outputs/
│   ├── sqlite/                     # Built databases
│   ├── validation/                 # Validation and parity reports
│   └── logs/                       # Run logs and warnings
├── docs/
│   └── etl_flowcharts.md           # Parameter-specific lineage reference
├── legacy_backend/                 # Read-mostly parity evidence
├── tests/                          # Focused and integration tests
└── pyproject.toml                  # uv dependencies and tool configuration
```

The main repository areas are:

- `config/`: paths, the external-source registry, scenario selections, and parameter
  rules or conversions;
- `src/fetching/`: source acquisition, physical validation, and normalization;
- `src/parameterization/`: parameter transformations as they are implemented;
- `src/validation/`: typed config, provenance, schema, insertion, integrity, and parity;
- `workflow/Snakefile`: coarse dependency and artifact orchestration;
- `inputs/` and `outputs/`: registered inputs, intermediate data, SQLite builds, logs,
  and validation reports.

For scenario fields and implemented switches, see
[`config/scenarios/README.md`](config/scenarios/README.md). For parameter-specific
lineage, assumptions, equations, and intended outputs, see
[`docs/etl_flowcharts.md`](docs/etl_flowcharts.md).

Stable repository policy is in [`AGENTS.md`](AGENTS.md). Multi-step implementation plans
follow [`.agents/PLANS.md`](.agents/PLANS.md); those files are contributor guidance, not
project orientation.

## Common commands

Run the repository doctor and focused validation:

```powershell
uv run python scripts/doctor.py --scenario config/scenarios/legacy_reproduction.yaml
uv run ruff check .
uv run pytest
```

Run the implemented source adapters directly. Add or retain `--no-download` for
deterministic cache-only execution:

```powershell
uv run python -m fetching.nrcan_ceud --scenario config/scenarios/legacy_reproduction.yaml --no-download
uv run python -m fetching.vehicle_population --scenario config/scenarios/legacy_reproduction.yaml --no-download
uv run python -m fetching.statcan_tables --scenario config/scenarios/legacy_reproduction.yaml --no-download
uv run python -m fetching.cer_enerfuture --scenario config/scenarios/legacy_reproduction.yaml --no-download
uv run python -m fetching.nlr_atb_autonomie --scenario config/scenarios/legacy_reproduction.yaml --no-download
```

Build the current validated SQLite bootstrap:

```powershell
uv run python src/build_transport.py --scenario config/scenarios/legacy_reproduction.yaml --overwrite
```

Inspect the current workflow DAG without executing it:

```powershell
uv run snakemake -n --snakefile workflow/Snakefile --config scenario=config/scenarios/legacy_reproduction.yaml --cores 1
```

Source adapters write normalized audit tables under `inputs/1_interim/`. Database,
validation, and log locations are configured in `config/paths.yaml` and the selected
scenario.

## Development approach

Development is validation-first: stabilize direct Python interfaces and deterministic
artifacts, test the changed seam, then connect stable stages through Snakemake. Baseline
work preserves legacy-equivalent assumptions until differences are explained and
documented.
