# CANOE Transportation Backend

The CANOE transportation backend converts heterogeneous transportation evidence into
validated parameter rows and scenario-ready SQLite databases for Canadian Open Energy Model 
running on the Temoa energy system optimization framework.

Version 2.0 replaces the legacy Excel-centred compiler with a
configuration-owned Python backend in which sources, assumptions, transformations,
validation results, and accepted differences can be inspected independently.

## Table of Contents

- [Backend at a glance](#backend-at-a-glance)
- [Backend architecture](#backend-architecture)
- [Input-parameter ETL flowcharts](#input-parameter-etl-flowcharts)
  - [Mermaid version](#mermaid-version)
  - [Flowchart legends](#flowchart-legends)
  - [`existing_capacity`](#existing_capacity)
  - [`demand`](#demand)
  - [`limit_annual_capacity_factor`](#limit_annual_capacity_factor)
  - [`lifetime_process` and `lifetime_survival_curve`](#lifetime_process-and-lifetime_survival_curve)
  - [`efficiency`](#efficiency)
  - [`cost_invest`](#cost_invest)
  - [`cost_variable`](#cost_variable)
- [Installation](#installation)
- [Common commands](#common-commands)
- [Development approach](#development-approach)

## Backend at a glance

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
    subGraphTitleMargin:
      top: 0
      bottom: 25
---
flowchart LR

  subgraph SCENARIO["`**Scenario configuration: *config/scenarios/example.yaml***<br>Sources, trajectories, regions, periods, outputs, and switches`"]
      subgraph SOURCES["`**Source registry: *config/sources.yaml***<br>Identity, edition, access, cache, citation, units, refresh, DQI`"]
            EVIDENCE@{shape: docs, label: "**External inputs: *inputs/0_/***<br>Public sources, cache, external models, and reviewed manual parameters"}
      end
      
      subgraph RULES["`**Harmonization rules: *config/parameters/rules.yaml***<br>Layouts, selectors, mappings, filters, bins, and expected artifacts`"]
            INTERIM@{shape: docs, label: "**Normalized inputs: *inputs/1_interim/***<br>Validated, auditable, source-shaped tables"}
            HARM@{shape: docs, label: "**Model parameters: *inputs/2_processed/***<br>Aggregated, Temoa-ready parameter rows with explicit lineage"}
      end
  end
  DB[("`**CANOE-transport database**<br>Validation and insertion into *canoe-schema* transport-sector database`")]

  EVIDENCE == fetching/ ==> INTERIM == parameterization/ ==> HARM == validation/ ==> DB
  
  INSIGHTS@{shape: processes, label: "**docs/**<br>Architecture, ETL flowcharts, assumptions, source inventory, and Marimo notebooks"}
  INSIGHTS -. diagnoses .-> INTERIM
  INSIGHTS -. explains .-> HARM
  INSIGHTS -. documents .-> DB
```

The [source registry](../config/sources.yaml) owns external identity and provenance;
[harmonization rules](../config/parameters/rules.yaml) own source-layout and
transformation contracts; reusable factors belong in
[conversion configuration](../config/parameters/conversion.yaml); and
[scenario YAML](../config/scenarios/legacy_reproduction.yaml) selects editions,
trajectories, regions, periods, outputs, and active switches. Canonical locations are
owned by [path configuration](../config/paths.yaml). Modular Python performs acquisition
and transformation, while Snakemake coordinates only stable dependencies and artifacts.
Pydantic and the pinned `canoe-schema` package form trust boundaries before atomic
SQLite publication.

The [backend architecture](backend_architecture.md) is the ownership reference. The
[ETL flowcharts](etl_flowcharts.md) document parameter-specific provenance,
harmonization, equations, and intended outputs; the
[source inventory](source_inventory.md) discusses source families without replacing the
registry. Legacy workbooks and databases remain read-mostly comparison evidence.
Marimo notebooks are the interactive diagnostics layer and may eventually expose
configuration choices to users who should not need to edit Python, but that layer is
currently focused rather than comprehensive.

Reproducibility is therefore collective: Git records reviewed changes; YAML owns
selectable configuration; documented paths separate cached, interim, processed, and
published artifacts; modular Python keeps transformations testable; manifests and logs
record execution; and validation and parity reports make each deterministic build
auditable.

## Backend architecture

The current and proposed repository layout provides the following navigation. The
focused structural reference is
[`docs/backend_architecture.md`](docs/backend_architecture.md).

```mermaid
---
config:
  treeView:
    showIcons: false
---
treeView-beta
  AGENTS.md ## Stable repository policy
  README.md ## Human-facing project orientation
  .agents/
      PLANS.md ## ExecPlan protocol
      plans/ ## Task-local implementation records
      skills/ ## Optional task-retrieved procedures
  config/
      paths.yaml ## Canonical directories and artifact paths
      sources.yaml ## External-source registry and provenance
      scenarios/ ## Scenario authoring contract
      parameters/
          rules.yaml ## Extraction and harmonization contracts
          conversion.yaml ## Reusable conversion factors
  workflow/
      Snakefile ## Dependency and artifact orchestration
  src/
      setup.py ## Load config, create paths, fetch/cache data, validate sources
      build_transport.py ## Build SQLite, run modules, post-process, log
      fetching/ ## Upstream download, cache, and interim normalization
          nrcan_ceud.py ## NRCan CEUD transport tables
          vehicle_population.py ## Provincial vehicle population reports
          statcan_tables.py ## Statistics Canada transport tables
          cer_enerfuture.py ## CER energy future tables
          nlr_atb_autonomie.py ## NLR ATB and ANL Autonomie inputs
          assorted_sources.py ## Smaller registered source adapters
      parameterization/ ## Transform normalized inputs into model parameters
          stocks_and_demands.py ## Capacity, demand, utilization, and anchors
          lifetimes_survival.py ## Lifetimes and survival curves
          road_aggregation.py ## Road class mappings and aggregation weights
          efficiencies.py ## Technology efficiencies
          capex_opex.py ## Investment and operating costs
          ldv_charging.py ## BEV charging profiles and time slices
          emissions.py ## Vehicle-cycle and operating emissions
          market_constraints.py ## Market shares, policy limits, and SCC rules
          adoption_constraints.py ## Adoption and growth constraints
          sector_coupling.py ## Fuel, electricity, hydrogen, and blends
      utils/ ## Typed config and path utilities
      validation/
          config_models.py ## Pydantic configuration contracts
          provenance.py ## Source and dataset provenance
          schema_contract.py ## canoe-schema v4 compatibility
          insertion.py ## Validated parameterized insertion
          database_bootstrap.py ## Integrity and publication checks
          legacy_compare.py ## Legacy SQLite comparison
  scripts/
      doctor.py ## Non-mutating repository readiness check
      clean_runtime.py ## Explicit runtime cleanup
  inputs/
      0_canoe_template/ ## Backend-owned structural templates
      0_cache/ ## Authoritative cached downloads
      0_external_models/ ## Registered external-model artifacts
      1_interim/ ## Normalized auditable tables
      2_processed/ ## Parameter-ready tables
  outputs/
      sqlite/ ## Built CANOE/Temoa-ready databases
      validation/ ## Validation and parity reports
      logs/ ## Run logs and warnings
  docs/
      backend_architecture.md ## Repository structure and ownership reference
      etl_flowcharts.md ## Parameter-specific lineage reference
  legacy_backend/ ## Read-mostly parity evidence
  tests/ ## Focused and integration tests
  pyproject.toml ## uv dependencies and tool configuration
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
[`config/scenarios/README.md`](config/scenarios/README.md).

Stable repository policy is in [`AGENTS.md`](AGENTS.md). Multi-step implementation
plans follow [`.agents/PLANS.md`](.agents/PLANS.md); these are contributor references
rather than project orientation.

## Input-parameter ETL flowcharts

These diagrams showcase the source relationships, harmonization intent, equations, and
target outputs for the transportation parameter families. The focused lineage reference
is [`docs/etl_flowcharts.md`](docs/etl_flowcharts.md).

**Important note:** if text inside nodes/figures does not render completely, you must reset your zoom to 100% and reload the website. Weird, I know.

### Mermaid version

```mermaid
info
```

### Flowchart legends

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  %% --- Sources ---
  s0[("`**Maintained sources**<br>Public datasets that are curated, maintained and updated regularly`")]
  s1@{shape: doc, label: "**Heterogeneous sources**<br>Public data that is manually scraped from documentation, model inputs, or assumptions"}
  s2@{shape: win-pane, label: "**External model outputs**<br>Soft-linked outputs from external models, can be entire models (e.g., GREET), or results alone (e.g., RAMP-mobility)" }

  %% --- Processes ---
  p1["`**Harmonization protocol**<br>• Briefly describes parameter-handling rules, declared in *config/parameters/rules.yaml*<br><br>• There can be several processes and/or rules, usually described in a table below the chart`"]

  s0 -- required process --> p1
  s1 -. conditional process .-> p1
  s2 --> p1

  p1 --> o1[/"`**Parameter-ready output**<br>Parameter values inserted into SQLite databases<br>[describes units]`"/]
```

### `existing_capacity`

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  %% --- Sources ---
  s0[("`**NRCan CEUD**<br>Provincial vehicle sales, stocks, off-road energy use, and energy intensities`")]
  s1[("`**ON Transportation**<br>Fit-active vehicle age cohort by inferred size class`")]

  subgraph expansion["`**Other provincial sources**`"]
    direction LR
    s2[("`**Quebec SAAQ**<br>Active vehicle age cohort by inferred size class`")]
    s3[("`**Insurance Corp. of BC**<br>Vehicle age cohort by size class`")]
  end

  s4[("`**StatCan table**<br>New LDV registrations by fuel type`")]
  s5[("`**StatCan table**<br>Vehicle registrations by fuel type`")]

  %% --- Processes ---
  p1["`**Fleet age distribution<br>**• *Road:* distribute stock by age<br><br>• *Off-road:* treat provincial energy use ÷ intensity as stock, then distribute by age`"]
  s0 -- nrcan_ceud.py --> p1
  s1 -- vehicle_population.py --> p1
  expansion -. "vehicle_population.py" .-> p1

  p2["`**Fleet powertrain distribution**<br>• *Road:* distribute age-specific stock by powertrain<br><br>• *Off-road:* incumbent techs mostly use diesel or jet fuel<br><br>• Aggregate into 5-year vintages`"]
  p1 -- stocks_and_demands.py --> p2
  s4 -- statcan_tables.py --> p2
  s5 -- statcan_tables.py --> p2

  p2 -- stocks_and_demands.py --> o1[/"`**existing_capacity**<br>[k vehicles]<br>[bn tonne-km]<br>[bn passenger-km]`"/]

  %% --- Hyperlinks ---
  click s5 "https://doi.org/10.25318/2310030801-eng"
  click s4 "https://doi.org/10.25318/2010002501-eng"
  click s3 "https://public.tableau.com/app/profile/icbc/viz/VehiclePopulationIntroPage/VehiclePopulationData"
  click s2 "https://www.donneesquebec.ca/recherche/dataset/vehicules-en-circulation"
  click s1 "https://data.ontario.ca/dataset/vehicle-population-data"
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
```

| Harmonization rule                                     | Affected classes      | Description                                                                                                                          |
| ------------------------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Fetch vehicle counts by inferred size class            | Cars and light trucks | Size classes are inferred from vehicle model and make                                                                                |
| Distribute stock by age                                | Road vehicles         | Distribute NRCan CEUD stocks over existing vintages using age cohort registrations                                                   |
| Treat energy use<sub>province</sub>÷intensity as stock | Off-road modes        | Air, rail and marine fleet size are estimated from the available supply capacity to satisfy demand by vintage (in demand units)      |
| Distribute stock by age                                | Off-road modes        | Available demand supply by vintage is estimated with a fleet turnover approximation assuming an avg. annual retirement of 1÷lifetime |
| Distribute stock<sub>age</sub> by powertrain           | Cars and light trucks | Each stock by vintage gets distributed over vehicle market shares by fuel type                                                       |
| Distribute stock<sub>age</sub> by powertrain           | MD trucks             | Each stock by vintage gets distributed over vehicle registration shares by fuel type – mainly diesel and gasoline                    |
### `demand`

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  %% --- Sources ---
  s0[("`**NRCan CEUD**<br>Provincial vehicle activity and off-road energy use; national off-road energy intensity`")]
  s1[("`**CER Canada's Energy Future**<br>Real GDP projections<br>*Def. scenario:* current measures`")]

  %% --- Processes ---
  p1["`**Baseline and projection**<br>• *Off-road:* estimate provincial activity as energy use ÷ intensity<br><br>• Index future demand to GDP growth by scenario`"]

  s0 -- nrcan_ceud.py --> p1
  s1 -- cer_enerfuture.py --> p1

  p1 -- stocks_and_demands.py --> o1[/"`**demand**<br>[bn passenger-km]<br>[bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s1 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
```

| Harmonization rule                                                      | Affected classes | Description                                                                                                    |
| ----------------------------------------------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------- |
| Estimate provincial activity as energy use<sub>province</sub>÷intensity | Off-road modes   | Provincial energy consumption [PJ] divided by national energy intensity [PJ/bn-tkm] as provincial demand proxy |
| Index future demand to GDP growth                                       | All              | Base year demand is indexed to future GDP growth projections by scenario                                       |
### `limit_annual_capacity_factor`

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  %% --- Sources ---
  s0[("`**NRCan CEUD**<br>Provincial vehicle activity [bn tonne-km] and stock [k vehicles]`")]
  s1[("`**NLR Annual Tech. Baseline**<br>Age-based annual mileage profiles (VMT schedules) of cars and LD, MD, and HD trucks`")]
  s2@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes"}

  %% --- Processes ---
  subgraph utilization[" "]
	  direction TB
	  p0["`**Annual vehicle utilization (UF)**<br>**eq. (i)** 5-year avg of activity ÷ stock excluding 2020-2021, then scaled by **capacity_to_activity**`"]
	  s0 -- nrcan_ceud.py --> p0
  end
  style utilization fill:transparent,color:transparent

  p1@{shape: hex, label: "**config/scenarios/**<br>*vkt_schedules:* true or false"}
  utilization -- stocks_and_demands.py --> p1

  p2["`**Flat utilization trajectory**<br>Assume constant annual vehicle utilization across all periods`"]
  p1 -- false --> p2

  o1[/"`**limit_annual_capacity_factor**<br>*Operator: =*<br>*Indexed by period*`"/]
  p2 -- stocks_and_demands.py --> o1

  p3["`**Age-based trajectory**<br>• Aggregate mileage profiles by vehicle class using mappings<br><br>• Normalize each profile by its maximum value<br><br>• Scale annual utilization by normalized age trajectory<br><br>• Aggregate utilization trajectories into 5-year periods`"]
  p1 -. true .-> p3
  s1 -- nlr_atb_autonomie.py --> p3
  s2 -- road_aggregation.py --> p3

  o2[/"`**limit_annual_capacity_factor**<br>*Operator: =*<br>*Indexed by vintage and period*`"/]
  p3 -. "stocks_and_demands.py" .-> o2

  %% --- Hyperlinks ---
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s1 "https://atb.nlr.gov/transportation/2024/data"
```
#### Equations

```math
(i)\;\mathrm{UF[\text{-}]}=\frac{\mathrm{Activity[bn\;tonne\text{-}km/year]}}{\mathrm{Stock[k\;units]}\cdot \mathrm{C2A[bn\;t\text{-}km/k\;units\cdot year]}}
```

| Harmonization rule                          | Affected classes | Description                                                                                                                                                           |
| ------------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Annual utilization as activity÷stock ratios | Road vehicles    | Represents how much activity a unit of capacity can deliver annually. Utilization is scaled with an arbitrary `capacity_to_activity` factor to avoid near-zero values |
| Assume constant annual utilization          | Road vehicles    | Annual utilization derived from 5-year average ratios remains constant across all periods                                                                             |
| Aggregate profiles and normalize            | Road vehicles    | Aggregate annual mileage profiles by vehicle size/weight class using aggregation mappings (see `efficiency` flowchart) and apply max scaling to each series           |
| Scale utilization by normalized trajectory  | Road vehicles    | Baseline utilization is indexed through normalized age trajectories to obtain utilization as a function of vehicle age, mostly decaying                               |

### `lifetime_process` and `lifetime_survival_curve`

```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  %% --- Sources ---
  subgraph national["`**National granularity (US)**`"]
    direction LR
    s2[("`**NHTSA CAFE model**<br>LDV survival rates by vehicle size class`")]
    s3[("`**EIA NEMS model**<br>MD/HD truck survival rates by weight class`")]
  end
  subgraph provincial["`**Provincial granularity (CA)**`"]
    direction LR
    s0[("`**ON Transportation**<br>Fleet age cohorts for survival-rate estimation`")]
    s1[("`**Quebec SAAQ**<br>Fleet age cohorts for survival-rate estimation`")]
  end

  %% --- Processes ---
  p1@{shape: hex, label: "**config/scenarios/**<br>*survival_curves:* true or false"}
  provincial -. "vehicle_population.py" .-> p1
  national -- planned source adapter --> p1

  s6@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, and MD/HD truck weight classes; see *efficiency*"}
  s4[("`**StatCan table**<br>Buses avg. lifetime by province`")]
  s5@{shape: doc, label: "**SFU CIMS model assumptions**<br>Lifetime of remaining modes"}

  p2["`**Road retirement profiles**<br>• **eq. (i)** If provincial sources, estimate fleet retirement rates by age with fleet cohort data<br><br>• Aggregate survival curves for cars and trucks, truncated to 30 years, using mappings`"]
  p1 -. true .-> p2
  s6 -. "road_aggregation.py" .-> p2

  p3["`**Fixed lifetimes**<br>• Aggregate survival rates of road vehicles using mappings<br><br>• Median lifetimes (p<sub>survival</sub>=0.5) by default when survival curves are disabled<br><br>• Get avg. lifetimes from remaining sources`"]
  p1 -- false --> p3
  s6 -- road_aggregation.py --> p3
  s4 -- statcan_tables.py --> p3
  s5 -- inputs/0_manual_params/ --> p3

  p2 -. "lifetimes_survival.py" .-> o1[/"`**lifetime_survival_curve**<br>[-]`"/]
  p3 -- lifetimes_survival.py --> o2[/"`**lifetime_process**<br>[years]`"/]

  %% --- Hyperlinks ---
  click s0 "https://data.ontario.ca/dataset/vehicle-population-data"
  click s1 "https://www.donneesquebec.ca/recherche/dataset/vehicules-en-circulation"
  click s2 "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/"
  click s3 "https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx"
  click s4 "https://doi.org/10.25318/3410025401-eng"
  click s5 "https://github.com/EMRG-SFU/cims-models/tree/main/sources/sectors"
```

#### Equations

```math
(i)\;\mathrm{Survival}_{age}=\frac{\mathrm{Stock}_{vintage,\;age}}{\mathrm{Stock}_{vintage,\;0}}
```

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps (see `efficiency`) | Cars, light trucks, and MD/HD trucks | Map source vehicle classes to the CANOE size and weight classes used to aggregate retirement profiles. |
| Survival curves are truncated to 30 years | Cars and trucks | Planned; the truncation rationale and implementation remain to be documented. |
| Median lifetimes compiled by default | All | When survival curves are disabled, use the median of a road-vehicle profile and configured average lifetimes for remaining classes. |

**Notes:**
- NHTSA CAFE model, used for cars and light trucks - survival rates table is inside parameters_ref.xlsx in 'Vehicle Age Data'!A3:E45, such file is downloaded at: <https://static.nhtsa.gov/nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/Central_Analysis_Inputs.zip>
- EIA NEMS model, used for medium and heavy trucks - survival rate table is inside trnhdv.xlsx in trnhdv!A86:D120, such file is downloaded from the NEMS repo: <https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx>
- Motorcycles, aircraft, rail, marine vessels, and other infrastructure use configured
  average or median lifetimes from `inputs/0_manual_params/`. Road technologies with
  survival curves also retain their median lifetimes there; a scenario-enabled curve
  supersedes the fixed-lifetime row for that technology.

### `efficiency`

*Note: Some technologies are not represented in this diagram; see
`config/parameters/rules.yaml` for implemented harmonization entries.*
```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  %% --- Sources ---
  subgraph agg["`**Road aggregation weights**`"]
	  s3@{shape: doc, label: "**Wards Intelligence**<br>Vehicle sales by vintage, make, and model"}
	  s4[("`**ON Transportation**<br>Fit-active vehicles by vintage, make, and model`")]
	  s10[("`**StatCan Tables**<br>Truck shipment distance and tonne-km where province is origin or destination`")]
	  subgraph expansion["`Other provincial sources`"]
      direction LR
	    s5[("`**Quebec SAAQ**<br>Vehicles in operation by vintage, make, and model`")]
	    s6[("`**Insurance Corp. of BC**<br>Vehicle counts by vintage, make, and model`")]
	  end
  end
  subgraph road["`**Road efficiencies**`"]
	  s1[("`**NRCan Fuel Consum. Ratings**<br>Car and light-truck ratings by make and model`")]
	  s2[("`**Autonomie TEA via NLR ATB**<br>Future vehicle efficiencies and powertrain multipliers<br>*Def. scenario:* conservative trajectory`")]
	  s2_2[("`**JGCRI GCAM model**<br>Motorcycle (>250 cc) efficiencies for Canada`")]
  end
  subgraph off["`**Off-road efficiencies**`"]
	  s7@{shape: docs, label: "**EPRI REGEN model assumptions**<br>Future multipliers for inter-city buses and off-road modes"}
	  s8@{shape: doc, label: "**EIA NEMS model assumptions**<br>Fuel consumption improvement of -1%/year for new jet aircrafts"}
  end
  s0[("`**NRCan CEUD**<br>Fleet energy intensity for trucks and off-road modes`")]
  s9[("`**NRCan CEUD**<br>Vehicle occupancy and payload factors`")]

  %% --- Processes ---
  p0["`**Road aggregation mapping**<br>• *LDVs:* map vehicle size classes and derive efficiency aggregation weights<br><br>• *MD/HD trucks:* map truck weight classes and derive efficiency aggregation weights<br><br>• *HD trucks:* derive regional- and long-haul activity weights`"]
	  s3 -- inputs/0_manual_params/ --> p0
  s4 -- vehicle_population.py --> p0
  expansion -. "vehicle_population.py" .-> p0
  s10 -- statcan_tables.py --> p0

  p2["`**Road baseline and indexing**<br>• *Existing LDVs:* aggregate fuel consumption ratings using mappings<br><br>• *Existing MD/HD trucks:* use incumbent fleet energy intensity<br><br>• *New road vehicles:* index existing efficiencies to aggregated future multipliers`"]
  s1 -- nrcan_ceud.py --> p2
  s2 -- nlr_atb_autonomie.py --> p2
  s2_2 -- planned source adapter --> p2
  p0 -- road_aggregation.py --> p2

  s9 -- nrcan_ceud.py --> p4

  p3["`**Off-road baseline and indexing**<br>• *Existing off-road modes:* use incumbent fleet energy intensity<br><br>• *New off-road modes:* index existing efficiencies to future multipliers`"]
  s7 -- inputs/0_manual_params/ --> p3
  s8 -- inputs/0_manual_params/ --> p3

  s0 -- nrcan_ceud.py --> p2 & p3

  p4["`**Period & unit harmonization**<br>• Aggregate existing efficiencies into 5-year vintages<br><br>• Convert to service-output efficiency using load factors`"]
  p2 -- efficiencies.py --> p4
  p3 -- efficiencies.py --> p4

  p4 -- efficiencies.py --> o1[/"`***efficiency***<br>[PJ/bn passenger-km]<br>[PJ/bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s6 "https://public.tableau.com/app/profile/icbc/viz/VehiclePopulationIntroPage/VehiclePopulationData"
  click s5 "https://www.donneesquebec.ca/recherche/dataset/vehicules-en-circulation"
  click s4 "https://data.ontario.ca/dataset/vehicle-population-data"
  click s9 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s7 "https://us-regen-docs.epri.com/v2025/assumptions/transportation.html#non-road-vehicles"
  click s8 "https://www.eia.gov/outlooks/aeo/assumptions/pdf/TDM_Assumptions.pdf"
  click s2 "https://atb.nlr.gov/transportation/2024/data"
  click s1 "https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64"
  click s10 "https://doi.org/10.25318/2310014201-eng"
  click s2_2 "https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy"
```

| Harmonization rule                                | Affected classes           | Description                                                                                                                                                                                                      |
| ------------------------------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Map size classes and derive aggregation weights   | Cars and light trucks      | Map vehicle make/model counts to size classes that align with NRCan efficiency ratings and Autonomie projections                                                                                                 |
| Map weight classes and derive aggregation weights | MD/HD trucks               | Map truck weight-rating counts to classes that align with Autonomie truck projection classes                                                                                                                     |
| Derive regional- and long-haul activity weights   | Heavy-duty trucks          | Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes                                                                                                        |
| Aggregate efficiency ratings using mappings       | Cars and light trucks      | Use size-class aggregation weights to convert model-level fuel consumption ratings into fleet-average efficiencies by powertrain                                                                                 |
| Use incumbent fleet energy intensity              | MD/HD trucks and off-road  | Use NRCan incumbent fleet energy intensities as proxies for existing technology efficiencies where fuel use is dominated by one fuel type                                                                        |
| Index existing efficiencies to future multipliers | All                        | Apply alternative-powertrain and future-period multipliers to existing efficiencies (e.g., 2030 battery-electric and 2040 fuel-cell multipliers)                                                                 |
| Special handling of buses                         | Transit, school, intercity | Use reported Autonomie values for existing and future transit and school bus efficiencies; use EPRI REGEN inputs for intercity buses                                                                             |
| Special handling of motorcycles                   | Motorcycles                | Use [PNNL GCAM](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) efficiencies |
| Convert to service-output efficiency units        | All                        | Convert source efficiencies (e.g., L/100 km or mpg) into demand units (e.g., bn tonne-km/PJ) with NRCan CEUD load factors; using HHVs.                                                                           |
### `cost_invest`

*Note: Some technologies are not represented in this diagram; see
`config/parameters/rules.yaml` for implemented harmonization entries.*
```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  s3[("`**CER Canada's Energy Future**<br>Currency exchange rates and GDP deflator index<br>*Def. scenario:* mid trajectory`")]
  subgraph offroad["`**Off-road CAPEX**`"]
	  %% --- Sources ---
	  s7@{shape: docs, label: "**SFU CIMS model assumptions**<br>Capital cost allocation of new off-road transportation in normalized units of demand"}
	  s8@{shape: docs, label: "**EPRI REGEN model assumptions**<br>Price trajectories of coach buses and CAPEX multipliers of alternative off-road modes"}
  end

  %% --- Processes ---
  p3["`**Cost of new off-road demand**<br>• Capital cost of building supply capacity to satisfy off-road demand *[dollars/demand unit]*<br><br>• Aircrafts' CAPEX normalized with utilization and load factors used in OPEX`"]
  s7 & s8 -- inputs/0_manual_params/ --> p3

  subgraph road["`**Road vehicle costs**`"]
	  %% --- Sources ---
	  s1@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
	  s2[("`**Autonomie TEA via NLR ATB**<br>Modeled vehicle price by class and powertrain<br>*Def. scenario:* conservative trajectory`")]
  end

  %% --- Processes ---
  p2["`**Vehicle manufacturing costs**<br>• Revert vehicle prices back to manufacturing costs, divide by the RPE markup factor of 1.5<br><br>• Aggregate manufacturing cost projections by vehicle class using efficiency mappings`"]
  s2 -- nlr_atb_autonomie.py --> p2
  s1 -- road_aggregation.py --> p2

  p4["`**Harmonize currency units**<br>• Apply exchange rate to CAD<br>(e.g., 2023USD → 2023CAD)<br><br>• Discount to reference year<br>(e.g., 2023CAD → 2020CAD)<br><br>• Harmonize magnitude of denominators`"]
  p2 -- capex_opex.py --> p4
  p3 -- capex_opex.py --> p4
  s3 -- cer_enerfuture.py --> p4

  p4 -- capex_opex.py --> o1[/"`***cost_invest***<br>[$M 2020CAD/k vehicles]<br>[$M 2020CAD/bn passenger-km]<br>[$M 2020CAD/bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s8 "https://us-regen-docs.epri.com/v2025/assumptions/transportation.html#non-road-vehicles"
  click s7 "https://github.com/EMRG-SFU/cims-models/tree/main/sources/sectors"
  click s2 "https://vms.taps.anl.gov/research-highlights/vehicle-technologies/u-s-doe-vto-hfto-r-d-benefits/"
  click s3 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
```

| Harmonization rule                                  | Affected classes                                                 | Description                                                                                                                                                                                                                                                                                                                         |     |
| --------------------------------------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| Reuse road aggregation maps                         | - Cars and light trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with NRCan efficiency ratings and Autonomie projections<br>- Map truck weight-rating counts to classes that align with Autonomie truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes |     |
| Revert vehicle prices back into manufacturing costs | Road vehicles                                                    | A manufacturing to retail price equivalent markup factor (RPE) of 1.5 is generally used in Autonomie TEA and TCO study series                                                                                                                                                                                                       |     |
| Aggregate manufacturing costs using mappings        | Cars and trucks                                                  | Use mapped weights to aggregate Autonomie projected manufacturing costs of cars, LD, MD, and HD trucks, and school and transit buses                                                                                                                                                                                                |     |
| CAPEX of new off-road demand capacity               | Off-road modes                                                   | Obtain input capital costs of building new supply capacity that satisfies off-road demand, derived as dollars per demand unit                                                                                                                                                                                                       |     |
| Special handling of buses                           | Intercity buses                                                  | Use EPRI REGEN vehicle purchase cost assumptions for intercity buses                                                                                                                                                                                                                                                                |     |
| Special handling of motorcycles                     | Motorcycles                                                      | Use [PNNL GCAM](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) purchase costs                                                                                                                  |     |
| Harmonize currency units                            | All                                                              | Convert values using foreign currencies and/or different dollar years into reference currency-year                                                                                        |

### `cost_variable`

*Note: Some technologies are not represented in this diagram; see
`config/parameters/rules.yaml` for implemented harmonization entries.*
```mermaid
---
config:
  layout: dagre
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
    curve: linear
---
flowchart LR
  subgraph offroad["`**Off-road OPEX**`"]
	  %% --- Sources ---
	  s7@{shape: docs, label: "**CMU OEO model assumptions**<br>Variable costs of rail techs set to 6% (freight) and 10% (passenger) of CAPEX; marine freight set to 5%"}
	  s8@{shape: docs, label: "**FAA Benefit-Cost Analysis**<br>• *Table 4-7 to 4-8:* Passenger & cargo aircraft avg. maintenance costs per block-hour<br>• *Table 3-6 to 3-10:* Median block speeds, aircraft capacities, and load factors"}
  end

  %% --- Processes ---
  p3["`**Variable costs from off-road**<br>• *Aircrafts:* **eq. (i-ii)** normalized maintenance costs per demand unit (CAPEX uses same factors) <br><br>• *Other off-road:* estimate variable costs with OEO ratios`"]
  s7 & s8 -- inputs/0_manual_params/ --> p3

  subgraph road["`**Road M&R costs**`"]
	  %% --- Sources ---
	  s1@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
	  s2[("`**NLR ATB (Burnham et al. 2021)**<br>Avg. maintainance costs per mile, size and powertrain multipliers, and repair cost coefficients for LDVs<br><br>**NLR ATB (Autonomie TEA)**<br>Modeled vehicle price by class and powertrain<br>*Def. scenario:* conservative trajectory`")]
	  s3@{shape: win-pane, label: "**ANL BEAN (Islam et al. 2022)**<br>Maintainance and repair linear model coefficients for MHDVs"}
  end

  %% --- Processes ---
  p2["`**Maintainance & repair costs**<br>• *LDVs:* **eq. (i)** get age-dependent repair cost via empirical model;<br>**eq. (ii)** add avg. maintenance costs per mile (Burnham et al. 2021)<br><br>• *MHDVs:* **eq. (iii)** age-dependent M&R costs via empirical model (Islam et al. 2022)<br><br>• Aggregate M&R cost-per-mile curves by vehicle class using efficiency mappings`"]
  s2 -- nlr_atb_autonomie.py --> p2
  s3 -- inputs/0_external_models/ --> p2
  s1 -- road_aggregation.py --> p2

  s0[("`**CER Canada's Energy Future**<br>Currency exchange rates and GDP deflator index<br>*Def. scenario:* mid trajectory`")]
  s9[("`**NRCan CEUD**<br>Vehicle occupancy and payload factors`")]
  p4["`**Harmonize currency units**<br>• Apply exchange rate to CAD<br>(e.g., 2023USD → 2023CAD)<br><br>• Discount to reference year<br>(e.g., 2023CAD → 2020CAD)<br><br>• Harmonize magnitude and units of denominators`"]
  p2 -- capex_opex.py --> p4
  p3 -- capex_opex.py --> p4
  s0 -- cer_enerfuture.py --> p4
  s9 -- nrcan_ceud.py --> p4

  p4 -- capex_opex.py --> o1[/"`***cost_variable***<br>[$M 2020CAD/k vehicles]<br>[$M 2020CAD/bn passenger-km]<br>[$M 2020CAD/bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s9 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s8 "https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost"
  click s7 "https://github.com/TemoaProject/oeo/blob/master/database_documentation/TransportationSector.ipynb"
  click s2 "https://atb.nlr.gov/transportation/2024/data"
  click s2_2 "https://anl.app.box.com/s/an4nx0v2xpudxtpsnkhd5peimzu4j1hk/folder/242640145714"
  click s0 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
  click s3 "https://vms.taps.anl.gov/research-highlights/vehicle-technologies/u-s-doe-vto-hfto-r-d-benefits/"
```

#### Road M&R equations

```math
(i)\; \mathrm{Repair}^{LDV}_{age}=\mathrm{size}\cdot \mathrm{pwt}\cdot C_{age}\cdot e^{\beta\cdot \mathrm{price}}
```

```math
(ii)\; \mathrm{M\&R}^{LDV}_{age}=\mathrm{Repair}^{LDV}_{age}+\mathrm{Maint.}^{LDV}
```

```math
(iii)\; \mathrm{M\&R}^{MHDV}_{age}=\mathrm{pwt}(m\cdot \mathrm{age}+b)
```

#### Off-road M&R equations

```math
(i)\; \mathrm{M\&R}^{Air}=\frac{\mathrm{Cost\;per\;block\text{-}hour}}{\mathrm{Block\;speed}\cdot \mathrm{Seats}\cdot \mathrm{Load\;factor}}
```

```math
(ii)\; \mathrm{M\&R}^{Air}=\frac{\mathrm{Cost\;per\;block\text{-}hour}}{\mathrm{Block\;speed}\cdot \mathrm{Tonnes}\cdot \mathrm{Load\;factor}}
```

| Harmonization rule                                  | Affected classes                                                 | Description                                                                                                                                                                                                                                                                                                                         |     |
| --------------------------------------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| Reuse road aggregation maps                         | - Cars and light trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with NRCan efficiency ratings and Autonomie projections<br>- Map truck weight-rating counts to classes that align with Autonomie truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes |     |
| Vehicle prices used in repair cost empirical model | Road vehicles                                                    | Minimum suggested retail prices (MSRP) are simply manufacturing cost outputs from Autonomie scaled evenly by a markup factor of 1.5; these are used for repair cost estimates, as done in Burnham et al. 2021                                                                                                                                                                                                       |     |
| Aggregate M&R costs using mappings        | Cars and trucks                                                  | Use mapped weights to aggregate M&R cost curves of cars, LD, MD, and HD trucks, and and transit buses                                                                                                                                                                                                |     |
| Variable costs from off-road               | Off-road modes                                                   | - Estimate annual variable costs of rail and marine vessels with CAPEX-to-OPEX ratios used in the OEO model<br> - Estimate M&R costs per demand unit with empirical data from the FAA, converting miles to km and tons to tonnes; same apply annual utilization and load factors are used for CAPEX and OPEX                                                                                                                                                                                                        |     |
| Special handling of buses                           | School and intercity buses                                                  | Assume same CAPEX-to-OPEX ratios as those from transit buses                                                                                                                                                                                                                                                                |     |
| Special handling of motorcycles                     | Motorcycles                                                      | Use [PNNL GCAM](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) M&R costs                                                                                                                  |     |
| Harmonize currency units                            | All                                                              | - Convert values using foreign currencies and/or different dollar years into reference currency-year<br>- Harmonize denominators; convert miles into km and multiply by NRCan CEUD load factors

## Installation

Create the reproducible project environment from the lockfile:

```powershell
uv sync --frozen
```

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
