---
title: Backend architecture
role: Structural design reference for repository layout and module ownership.
retrieve_when: A task affects repository structure, module ownership, orchestration seams, or artifact placement.
read_scope: Read only the relevant tree branches and descriptions.
verify: Check planned content against current code, config, tests, schemas, and validation evidence before implementation.
---

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
│   ├── scenarios/                  # Scenario authoring contract
│   └── parameters/
│       ├── rules.yaml              # Extraction and harmonization contracts
│       └── conversion.yaml         # Reusable conversion factors
├── workflow/
│   └── Snakefile                   # Dependency and artifact orchestration
├── src/
│   ├── setup.py                    # Load config, create paths, fetch/cache data, validate sources
│   ├── build_transport.py          # Build SQLite, run modules, post-process, log
│   ├── fetching/                   # Upstream download, cache, and interim normalization
│   │   ├── nrcan_ceud.py           # NRCan CEUD transport tables
│   │   ├── vehicle_population.py   # Provincial vehicle population reports
│   │   ├── statcan_tables.py       # Statistics Canada transport tables
│   │   ├── cer_enerfuture.py       # CER energy future tables
│   │   ├── nlr_atb_autonomie.py    # NLR ATB and ANL Autonomie inputs
│   │   └── assorted_sources.py     # Smaller registered source adapters
│   ├── parameterization/           # Transform normalized inputs into model parameters
│   │   ├── manual_parameters.py    # Resolve compact manual category/powertrain selectors
│   │   ├── stocks_and_demands.py   # Capacity, demand, utilization, and anchors
│   │   ├── lifetimes_survival.py   # Lifetimes and survival curves
│   │   ├── road_aggregation.py     # Road class mappings and aggregation weights
│   │   ├── efficiencies.py         # Technology efficiencies
│   │   ├── capex_opex.py           # Investment and operating costs
│   │   ├── ldv_charging.py         # BEV charging profiles and time slices
│   │   ├── emissions.py            # Vehicle-cycle and operating emissions
│   │   ├── market_constraints.py   # Market shares, policy limits, and SCC rules
│   │   ├── adoption_constraints.py # Adoption and growth constraints
│   │   └── sector_coupling.py      # Fuel, electricity, hydrogen, and blends
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
│   ├── 0_manual_params/            # Review-owned compact manual parameter tables
│   ├── 1_interim/                  # Normalized auditable tables
│   └── 2_processed/                # Parameter-ready tables
├── outputs/
│   ├── sqlite/                     # Built CANOE/Temoa-ready databases
│   ├── validation/                 # Validation and parity reports
│   └── logs/                       # Run logs and warnings
├── docs/
│   ├── backend_architecture.md     # Repository structure and ownership reference
│   └── etl_flowcharts.md           # Parameter-specific lineage reference
├── legacy_backend/                 # Read-mostly parity evidence
├── tests/                          # Focused and integration tests
└── pyproject.toml                  # uv dependencies and tool configuration
```
