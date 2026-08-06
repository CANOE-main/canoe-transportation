# Transportation source inventory

[The source registry](../config/sources.yaml) is authoritative for identity, version,
access, cache registration, citation, validation expectations, and reviewed data quality.
This page is a discussion aid, not a second registry. Families below consolidate
components such as individual tables, editions, and manual-parameter selectors.

Sizes are coarse totals for the relevant cached or registered artifacts found in the
current workspace on 2026-08-06. **Current status** is evidence-based: it reflects code,
manifests, interim outputs, and reconciliation artifacts. It does not merely repeat the
registry's literal `status` field, which is currently `pending` for all 21 entries and
needs a defined lifecycle meaning. Scenario activation is a separate run choice.

| Source family | Main transport use | Access method | Native format/artifacts | Approximate size | Current status |
|---|---|---|---|---:|---|
| NRCan Comprehensive Energy Use Database (provincial and national) | Activity, stock, distance, fuel use, and energy intensity for road and non-road modes | Direct URL-templated downloads | Legacy XLS tables | `<10 MB` provincial; national `Not yet measured` | **In progress** — provincial cache and interim manifest exist; the required national cache/normalization is not currently evidenced |
| Ontario Ministry of Transportation vehicle population | Vehicle class, make/model/vintage, status, weight classes, stock mapping, and retention diagnostics | Ontario CKAN package API with annual-resource discovery | Annual ZIP archives containing text reports | `~10–100 MB` | **Implemented** — historical caches and source-normalized Reports A, 4, and 5 artifacts exist; reviewed mapping remains partial |
| Statistics Canada transportation tables | LDV registrations, trucking activity/fuels, freight candidates, and bus expected life | Web Data Service metadata plus full-table download API | ZIP archives with CSV data and JSON metadata | `~10–100 MB` | **Implemented** — five registered tables have cached artifacts, normalized outputs, and an `ok` manifest |
| Canada Energy Regulator, Canada's Energy Future | Macroeconomic factors, transport demand, and gasoline/diesel prices by edition/scenario | Direct edition-specific Open Government CSV links | CSV files | `~10–100 MB` | **Implemented** — 2023 and 2026 cached/interim editions have `ok` manifests |
| NLR Transportation Annual Technology Baseline 2024 | Vehicle cost, range, efficiency, PHEV utility weighting, maintenance, and age-VMT evidence | Direct versioned ZIP download | Large ZIP containing source workbooks/tables | `~100 MB–1 GB` | **Implemented** — normalized outputs and manifest exist; reconciliation differences remain explicit |
| ANL Autonomie and BEAN 2022 | External-model maintenance coefficients and supporting vehicle analysis | Registered manual Box-folder acquisition | XLSX/XLSM workbooks and PDF report | `~10–100 MB` | **Implemented** — registered external-model folder and normalized maintenance artifact are present |
| NRCan Fuel Consumption Ratings | Historical ICE, BEV, and PHEV make/model efficiency and vehicle-class evidence | Direct Open Government resource downloads | CSV snapshots | `<10 MB` | **Implemented** — six pinned snapshots, normalized outputs, and a manifest are present |
| NHTSA CAFE 2024 Central Analysis | LDV survival-rate evidence | Direct pinned ZIP download | ZIP with Central Analysis workbook inputs | `<10 MB` | **Implemented** — cached and normalized by the assorted-sources adapter |
| EIA NEMS transportation inputs | Medium/heavy truck scrappage schedules | Immutable Git LFS media at a pinned commit | XLSX workbook | `<10 MB` | **Implemented** — pinned cache and source-normalized interim table are present |
| JGCRI GCAM core transport data | Canadian motorcycle stock, load factor, and lifetime inputs | Immutable raw Git file at a pinned commit | CSV | `<10 MB` | **Implemented** — pinned cache and source-normalized interim table are present |
| EPRI US-REGEN v2025 transportation | Intercity-bus evidence and reviewed non-road cost/efficiency multipliers | VuePress HTML/JavaScript snapshot plus version-controlled manual CSV selectors | HTML, JavaScript payload, and CSV rows | `<10 MB` | **Implemented** — normalized bus data and manual-selector reconciliation exist; configured exclusions/unresolved selectors remain visible |
| FAA Economic Values 2024 | Aircraft capacity, load, speed, and maintenance-cost evidence | Direct component PDF downloads | PDF report sections | `<10 MB` | **Implemented** — normalized outputs exist; the unresolved dollar year is retained as a warning |
| Wards Intelligence sales shares | Reviewed Canadian car/light-truck class market shares | Review-owned, version-controlled aggregate extraction | Compact CSV derived from proprietary workbook evidence | `<10 MB` | **Implemented** — 2018/2021 aggregate rows are registered and reconciled; no line-level proprietary records are published |
| CIMS model assumptions | Service output, capital-cost multipliers, and process lifetimes | Review-owned, version-controlled manual parameters | Compact CSV rows | `<10 MB` | **Implemented** — registered selectors and technology-resolution artifacts exist |
| Open Energy Outlook | Variable-cost multipliers and process lifetimes | Review-owned, version-controlled manual parameters | Compact CSV rows | `<10 MB` | **Implemented** — registered selectors and technology-resolution artifacts exist |
| Argonne R&D GREET 2025 Rev.1 | Reviewed marine HFO-to-MDO energy-intensity ratio | Review-owned CSV selector backed by locally retained workbooks | CSV parameter row; XLSM/XLA workbook set retained as evidence | `~10–100 MB` | **Implemented** — the reviewed ratio is registered and resolved; the large workbooks are evidence, not runtime row-level inputs |
| EPA MOVES4 population and activity | Heavy-duty truck process lifetime | Review-owned, version-controlled manual parameter | Compact CSV row derived from report evidence | `<10 MB` | **Implemented** — registered selector and technology-resolution artifact exist |
| Canada Energy Policy Simulator v3.4.7 | Motorcycle process lifetime | Review-owned, version-controlled manual parameter | Compact CSV row | `<10 MB` | **Implemented** — registered selector and technology-resolution artifact exist |
| Argonne HDSAM v4.5 | Hydrogen dispenser process lifetime | Review-owned, version-controlled manual parameter | Compact CSV row | `<10 MB` | **Implemented** — registered selector and technology-resolution artifact exist |
| FuelEconomy.gov vehicle data | Additional make/model/year vehicle-class evidence for mapping review | Direct download pinned by hash and byte count | ZIP containing `vehicles.csv` | `<10 MB` | **Implemented** — pinned cache, four-column normalized evidence, and manifest are present |

## Boundary notes

- [Backend-owned templates](../inputs/0_canoe_template/) define structural technology,
  commodity, region, and period rows. They are **not external sources** and therefore do
  not receive external citations, data-quality scores, or `Txx` identifiers.
- Authoritative downloads live under [`inputs/0_cache/`](../inputs/0_cache/), registered
  external-model artifacts under [`inputs/0_external_models/`](../inputs/0_external_models/),
  and reviewed manual tables under [`inputs/0_manual_params/`](../inputs/0_manual_params/).
  Fetchers publish auditable normalized tables to [`inputs/1_interim/`](../inputs/1_interim/).
- `inputs/2_processed/` is the intended parameter-ready stage but is currently empty.
  Presence in this inventory therefore does not imply insertion into the current SQLite
  database. Run-specific activation is controlled by scenario YAML, with
  [`legacy_reproduction.yaml`](../config/scenarios/legacy_reproduction.yaml) as the
  current authoring example.
