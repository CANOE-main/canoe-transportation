---
title: Transportation parameter ETL lineage
role: Parameter-specific ETL lineage, harmonization, equations, assumptions, and intended outputs.
retrieve_when: A task affects a named parameter family or its direct shared dependency.
read_scope: Read only the relevant parameter sections and local dependencies.
verify: Check planned content against current code, config, tests, schemas, and validation evidence before implementation.
---

Solid paths describe the default lineage and dashed paths describe conditional or
scenario-dependent alternatives. File and config labels identify the intended owner of
each operation. The legend below is local to these diagrams.

### Table of Contents

- [Flowchart legends](#flowchart-legends)
- [`existing_capacity`](#existing_capacity)
- [`demand`](#demand)
- [`limit_annual_capacity_factor`](#limit_annual_capacity_factor)
- [`lifetime_tech`, `lifetime_process` and `lifetime_survival_curve`](#lifetime_tech-lifetime_process-and-lifetime_survival_curve)
- [`efficiency`](#efficiency)
- [`cost_invest`](#cost_invest)
- [`cost_variable`](#cost_variable)
- [`emission_embodied`](#emission_embodied)
- [EV charger parameters](#ev-charger-parameters)
- [`limit_new_capacity_share` for LDV PEV range composition](#limit_new_capacity_share-for-ld-pev-range-composition)
- [`capacity_factor_tech` for BEV charging profiles](#capacity_factor_tech-for-bev-charging-profiles-pending-refactor)

### Flowchart Legends

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s0[("`**Maintained sources**
    Public datasets that are curated, maintained and updated regularly`")]
  s1@{shape: doc, label: "**Heterogeneous sources**
    Public data that is manually scraped from documentation, model inputs, or assumptions"}
  s2@{shape: win-pane, label: "**External model outputs**
    Soft-linked outputs from external models, can be entire models (e.g., GREET), or results alone (e.g., RAMP-mobility)" }

  %% ############ Processes ###########
  p0@{shape: hex, label: "**Conditional switch**
    *feature:* true or false"}
  p0_2@{shape: hex, label: "**Scenario selector**
    *scenario:* current measures, net-zero, etc."}
  subgraph process["`**Group of sources or processes**`"]
    direction TB
    p1["`**Harmonization protocol**
      • Briefly describes parameter-handling rules, declared in *config/parameters/rules.yaml*<br>
      • There can be several processes and/or rules, usually described in a table below the chart`"]
    p2@{shape: notch-rect, label: "**Marimo diagnostic notebook**
      Visualizes and compares inputs and evidence, and tests assumptions during development to diagnose ETL decisions and derive insights from sources"}
    p1 -- marimo_notebook.py --> p2
  end

  s0 -- required process ---> process
  s1 -. conditional process .-> p0 -. true .-> process
  s2 -- required process --> p0_2 --> process

  process --> o1[/"`**Parameter-ready output**
    Parameter values inserted into SQLite databases
    [describes units]`"/]
```

### `existing_capacity`

!!! note "Read capacity together with its lifetime representation"
    The [ageing chapter](model/ageing.md) distinguishes acquired vehicles, physical survivors and capacity supplied to Temoa. The selected representation governs the exported denominator. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s0[("`**NRCan CEUD**
    Provincial vehicle sales, stocks, off-road energy use, and energy intensities`")]

  subgraph expansion["`**Vehicle population**`"]
    direction LR
    s1[("`**ON Ministry of Transport (MTO)**
      • *Report A:* vehicle counts by make-model codes, model year, and status
      • *Report 5:* counts by aggregated class (COMM, PASS, BUS), model year, and status`")]
    s2[("`**Quebec SAAQ #to-do**
      Active vehicle counts by make-model, model year, and jurisdiction`")]
    s3[("`**Insurance Corp. of BC #to-do**
      Vehicle counts by make-model, vintage, and size class; aggregated and indexed by two attributes at a time`")]
  end

  subgraph classes["`**Vehicle class mapping**`"]
    s1_2[("`**NRCan Fuel Consum. Ratings**
      Official vehicle make and model mappings into size classes`")]
    s1_3[("`**EPA Fuel Economy API**
      Supporting vehicle make and model mappings into size classes`")]
  end

  s4[("`**StatCan table**
    New LDV registrations by fuel type and province`")]
  s5[("`**StatCan table**
    MHDV registrations by fuel type and province`")]
  s6[("`**Transport Canada (TC) EV Dashboard**
    Latest quarterly medium- and heavy-duty EV market shares by province; annual shares not disclosed by province`")]

  %% ############ Processes ###########
  %% #to-review: Updated production cohort preparation.
  p1["`**Annual fleet cohorts**
    • *Cars and trucks:* reconstruct sales and survival; scale to NRCan CEUD stock at the base year<br>
    • *Buses and motorcycles:* distribute stocks by MTO age evidence<br>
    • *Off-road:* treat provincial energy use ÷ intensity as stock, then distribute by age`"]

  p1_2@{shape: notch-rect, label: "**Vehicle population mapping diagnosis**
    MTO make-model codes are difficult to map. This notebook visualizes:<br>
    • mapped fit-active stock
    • stock that cannot be mapped reliably
    • vehicle class and vintage weights
    • Report A vs Report 5 age cohorts
    • MTO stock vs NRCan CEUD data
    • survival rates from mapped cohorts"}
  p1 -- vehicle_population_diagnosis.py --> p1_2
  s0 -- nrcan_ceud.py --> p1
  s1 -- vehicle_population.py --> p1
  s1_2 -- nrcan_ceud.py --> p1
  s1_3 -- fueleconomy_vehicles.py
  vpic_model_years.py
  vpic_vehicle_types.py --> p1

  %% #to-review: Preserve annual powertrain allocation in both lifetime modes.
  p2["`**Fleet powertrain distribution**
    • *Cars and trucks:* apply annual registration shares to sales and surviving stock<br>
    • *Off-road:* incumbent techs mostly use diesel or jet fuel<br>
    • Aggregate into 5-year vintages`"]
  p1 -- road_aggregation.py --> p2
  s4 -- statcan_tables.py --> p2
  s5 -- statcan_tables.py --> p2
  s6 -- assorted_sources.py --> p2

  %% #to-review: Shared source-tail rule and capacity basis.
  s7@{shape: processes, label: "**Road survival curves**
    NHTSA & NEMS, or MOVES; extend the last retirement rate to the selected survival threshold, then zero"}
  s7 -- road_lifetimes_survival.py --> p1

  p3["`**Existing fleet capacity**
    • *Curves off:* surviving stock with fixed-lifetime allocation<br>
    • *Curves on:* sales-equivalent capacity with annual survival and explicit lifetimes<br>
    • Keep physical survivors for utilization and efficiency`"]
  p2 -- build_existing_capacity.py --> p3 --> o1[/"`**existing_capacity**
    [k vehicles]
    [bn tonne-km]
    [bn passenger-km]`"/]

  %% --- Hyperlinks ---
  click s5 "https://doi.org/10.25318/2310030801-eng"
  click s4 "https://doi.org/10.25318/2010002501-eng"
  click s3 "https://public.tableau.com/app/profile/icbc/viz/VehiclePopulationIntroPage/VehiclePopulationData"
  click s2 "https://www.donneesquebec.ca/recherche/dataset/vehicules-en-circulation"
  click s1 "https://data.ontario.ca/dataset/vehicle-population-data"
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s1_2 "https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64"
  click s1_3 "https://www.fueleconomy.gov/feg/ws/index.shtml"
  click s6 "https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard"
```

| Harmonization rule                                     | Affected classes      | Description                                                                                                                          |
| ------------------------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Fetch vehicle counts by inferred size class            | Cars and light trucks | Map make-model-vintage keys with the reviewed NRCan and [*US FuelEconomy.gov*](https://www.fueleconomy.gov/feg/ws/index.shtml) evidence crosswalk                                       |
| Reconstruct and calibrate cohorts | Cars and trucks | `k = CEUD reference stock / sum(sales * survival)`, one factor per region/class; preserve annual acquisitions and survivors separately. [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) stock-age allocation is diagnostic only. #to-review |
| Retain stock-age allocation | Buses and motorcycles | Keep their existing [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) age distributions and fixed lifetime treatment. #to-review |
| Treat energy use<sub>province</sub>÷intensity as stock | Off-road modes        | Air, rail, and marine fleet size are estimated from the available supply capacity to satisfy demand by vintage (in demand units)      |
| Distribute stock by age                                | Off-road modes        | Available demand supply by vintage is estimated with a fleet turnover approximation assuming an avg. annual retirement of 1÷lifetime |
| Distribute stock<sub>age</sub> by powertrain           | Cars and light trucks | Each stock by vintage gets distributed over vehicle market shares by fuel type                                                       |
| Distribute stock<sub>age</sub> by powertrain           | MD trucks             | Each stock by vintage gets distributed over vehicle registration shares by fuel type – mainly diesel and gasoline                    |

#### Existing period handling
This example uses five-year vintage bins and the selected 2023 observation year:

| `period_mode: legacy` | `period_mode: prospective` |
| --------------------- | --------------------- | 
| - 2014 → 2015<br>- 2015 → 2015<br>- 2016 → 2020<br>- 2019 → 2020<br>- 2020 → 2020<br>- 2021 → 2023<br>- 2023* → 2023<br> | - 2014 → 2010<br>- 2015 → 2010<br>- 2016 → 2015<br>- 2019 → 2015<br>- 2020 → 2015<br>- 2021 → 2020<br>- 2023* → 2020<br> |

*The example uses the registered 2023 observation endpoint.*

### `demand`

!!! note "Keep observations and projected service separate"
    The [activity chapter](model/activity.md) explains the observed-year anchor, GDP growth and optional car/light-truck allocation. Parameter extension is not a new activity observation. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s0[("`**NRCan CEUD**
    Provincial vehicle activity and off-road energy use; national off-road energy intensity`")]
  s1[("`**CER Canada's Energy Future**
    Real GDP projections by scenario
    ***Def. scenario:*** current measures`")]

  %% ############ Processes ###########
  p0@{shape: hex, label: "**config/scenarios/**
    *future_car_demand:* GDP-indexed or extrapolated<br>
    When extrapolated, light trucks carry the residual GDP-indexed demand from both passenger LDV classes"}
  p0_2@{shape: hex, label: "**config/scenarios/**
    *cer_scenario:* current, higher, lower, or net-zero"}
  p1["`**Baseline and projection**
    • *Off-road:* estimate provincial activity as energy use ÷ intensity<br>
    • Index future demand to GDP growth by scenario`"]

  s0 -- nrcan_ceud.py --> p0 --> p1
  s1 -- cer_enerfuture.py --> p0_2 --> p1
  p1 -- build_demand.py --> o1[/"`**demand**
    [bn passenger-km]
    [bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s1 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
```

| Harmonization rule                                                      | Affected classes | Description                                                                                                    |
| ----------------------------------------------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------- |
| Estimate provincial activity as energy use<sub>province</sub>÷intensity | Off-road modes   | Provincial energy consumption [PJ] divided by national energy intensity [PJ/bn-tkm] as provincial demand proxy |
| Index future demand to GDP growth                                       | All              | Base year demand is indexed to future GDP growth projections by scenario                                       |

### `limit_annual_capacity_factor`

!!! warning "Mileage profiles do not imply emitted age-dependent factors"
    The current schema emits the supported flat class baseline. Age-dependent mileage profiles remain diagnostic audit products until a joint period/vintage interface is available. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s0[("`**NRCan CEUD**
    Provincial vehicle activity [bn tonne-km] and stock [k vehicles]`")]
  s0_2@{shape: doc, label: "***capacity_to_activity***
    Theoretical upper-bound representing annual activity delivered by a thousand vehicles at 100 km/h for 8,760 h/year"}
  s1[("`**NLR Annual Tech. Baseline**
    Age-based annual mileage profiles (VMT schedules) of cars and LD/MD/HD trucks`")]
  s2@{shape: processes, label: "**Road aggregation maps**
    Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}

  %% ############ Processes ###########
  p0["`**Annual vehicle utilization (UF)**
    **eq. (i)** Cars/trucks: base-year activity divided by calibrated physical surviving stock and **capacity_to_activity**. Other classes retain the observed-year mean. #to-review`"]
  s0 -- nrcan_ceud.py --> p0
  s0_2 -- inputs/0_manual_params/ --> p0

  p1@{shape: hex, label: "**config/scenarios/**
    *vkt_schedules:* true or false"}
  p0 -- road_stocks_and_demands.py --> p1

  p2["`**Flat utilization trajectory**
    Assume constant annual vehicle utilization across all periods`"]
  p1 -- false --> p2

  o1[/"`**limit_annual_capacity_factor**
    *Operator: =*
    *Indexed by vintage*`"/]
  p2 -- road_utilization.py --> o1

  p3["`**Age-based trajectory**
    • Aggregate mileage profiles by vehicle class using mappings<br>
    • Normalize each profile by its maximum value<br>
    • Scale annual utilization by normalized age trajectory<br>
    • Aggregate utilization trajectories into 5-year periods`"]
  p1 -. true .-> p3
  s1 -- nlr_atb_autonomie.py --> p3
  s2 -- road_aggregation.py --> p3

  o2[/"`**limit_annual_capacity_factor**
    *Operator: =*
    *Indexed by vintage and period*
    #to-do: must be reconciled with canoe-schema`"/]
  p3 -. "road_utilization.py" .-> o2

  %% --- Hyperlinks ---
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s1 "https://atb.nlr.gov/transportation/2024/data"
```

#### Equations

```math
(i)\;\mathrm{UF[\text{-}]}=\frac{\mathrm{Activity[bn\;tonne\text{-}km/year]}}{\mathrm{Stock[k\;units]}\cdot \mathrm{C2A[bn\;t\text{-}km/k\;units\cdot year]}}
```

**UF** is the dimensionless annual utilization fraction; **Activity** is annual
service in billion passenger-km or tonne-km; **Stock** is thousand physical
vehicles; and **C2A** is the capacity-to-activity scaling constant in billion
service-km per thousand vehicles per year. #to-review

| Harmonization rule                          | Affected classes | Description                                                                                                                                                           |
| ------------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Annual utilization as activity÷stock ratios | Road vehicles    | Represents how much activity a unit of capacity can deliver annually. Utilization is scaled with an arbitrary `capacity_to_activity` factor to avoid near-zero values |
| `capacity_to_activity` | Road vehicles    | An inconsequential scaling constant that represents the theoretical upper-bound of annual activity delivered by a thousand vehicle units operating year-round. |
| Assume constant annual utilization          | Road vehicles    | The emitted class-level baseline applies in every active operating period. Existing technologies are keyed to actual positive stock cohorts in each region; new technologies are keyed to model vintages. |
| Aggregate profiles and normalize            | Road vehicles    | Aggregate annual mileage profiles by vehicle size/weight class using aggregation mappings (see `efficiency` flowchart) and apply max scaling to each series           |
| Retain age-dependent mileage evidence | Road vehicles | Prepare normalized mileage trajectories as audit artifacts; emitted annual utilization remains the class-level baseline because the schema cannot index period and vintage together. #to-review |

### `lifetime_tech`, `lifetime_process` and `lifetime_survival_curve`

!!! warning "Registration changes are apparent retention"
    [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) edition-to-edition counts mix retirement with administrative changes and migration. Source schedules and their extrapolated tails remain distinct from those diagnostic observations. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  %% Source profile selection includes MOVES and NHTSA/NEMS. #to-review
  subgraph survival["US survival schedules"]
    direction LR
    s2[("`**NHTSA CAFE model**
      Car, light- and medium-truck survival rates`")]
    s3[("`**EIA NEMS model**
      HD truck scrappage rates (Class 7–8)`")]
    s7[("`**EPA MOVES6 model**
      Cars, light and heavy truck survival rates (Table C-1)`")]
  end

  subgraph population["Population evidence"]
    direction LR
    s0[("`**ON Ministry of Transport (MTO)**
      • *Report A:* vehicle counts by make-model codes, model year, and status`")]
    s1[("`**Quebec SAAQ #to-do**
      Active vehicle counts by make-model, model year, and jurisdiction`")]
  end

  s6@{shape: processes, label: "**Road aggregation maps**
    Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}

  %% ############ Processes ###########
  p4@{shape: hex, label: "**config/scenarios/**
    *road_lifetime_survival_source:*
    NHTSA CAFE & EIA NEMS, or EPA MOVES6 rates"}
  s2 & s3 & s7 -- "assorted_sources.py" --> p4

  p0["`**Survival rate estimation**
    • **eq. (i)** Estimate apparent retirement from make-model-vintage cohorts across Report A editions before mapping<br>
    • The MTO make-model mapping separates LDV stock exposure from unmapped and non-LDV; and aggregates evidence accordingly<br>
    • **eq. (ii)** The empirical rates by class and age are the total apparent retirement (*D*) divided by total starting exposure (*E*)<br>
    • Production uses the selected NHTSA/NEMS or MOVES profile; MTO estimates remain diagnostic #to-review`"]
  p4 -- "road_lifetimes_survival.py" --> p0
  s0 -. "`vehicle_population.py
    *\*diagnostic-only*`" .-> p0
  s6 -- road_aggregation.py --> p0

  p0_2@{shape: notch-rect, label: "**Vehicle population mapping diagnosis**
    MTO make-model codes are difficult to map. This notebook visualizes:<br>
    • mapped fit-active stock
    • stock that cannot be mapped reliably
    • vehicle class and vintage weights
    • Report A vs Report 5 age cohorts
    • MTO stock vs NRCan CEUD data
    • survival rates from mapped cohorts"}
  p0 -- vehicle_population_diagnosis.py --> p0_2

  p1@{shape: hex, label: "**config/scenarios/**
    *survival_curves:* true or false"}
  p0 -- road_lifetimes_survival.py --> p1

  s4[("`**StatCan table**
    Buses avg. lifetime by province`")]
  s5@{shape: doc, label: "**SFU CIMS model assumptions**
    Lifetime of remaining modes"}

  p2@{shape: hex, label: "**config/scenarios/**
    *tail_derivation:* constant_retirement<br>
    *tail_survival_threshold:* 0.005 (0.5%)<br>
    *survival_curve_max_age:* optional hard zero cap; null derives the end age #to-review"}
  p1 -. true .-> p2

  p3["`**Fixed lifetimes**
    • Median lifetimes (p<sub>survival</sub>=0.5) by default when survival curves are disabled<br>
    • Get avg. lifetimes from remaining sources`"]
  p1 -- false --> p3
  s4 -- statcan_tables.py --> p3
  s5 -- inputs/0_manual_params/ --> p3

  p2 -. true .-> pt["`**Annual reconciled curves**
    Extend the last annual retirement rate to 0.5% survival, then zero; use the same schedule for stock calibration and vintage curves #to-review`"]
  pt -. "build_lifetime_parameters.py" .-> ol[/"`**lifetime_process**
    Explicit terminal year minus model vintage [years] #to-review`"/]
  pt -. "build_lifetime_parameters.py" .-> o1[/"`**lifetime_survival_curve**
    [-]`"/]
  p3 -- build_lifetime_parameters.py
  offroad_lifetimes.py --> o2[/"`**lifetime_tech**
    [years]`"/]

  %% --- Hyperlinks ---
  click s0 "https://data.ontario.ca/dataset/vehicle-population-data"
  click s1 "https://www.donneesquebec.ca/recherche/dataset/vehicules-en-circulation"
  click s2 "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/"
  click s3 "https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx"
  click s7 "https://nepis.epa.gov/Exe/ZyPDF.cgi?Dockey=P101CUN7.pdf#page=192"
  click s4 "https://doi.org/10.25318/3410025401-eng"
  click s5 "https://github.com/EMRG-SFU/cims-models/tree/main/sources/sectors"
```

#### Equations

```math
(i)\quad r^{\mathrm{MTO}}_{p,k,v,a}=\frac{N_{p,k,v,a+1}}{N_{p,k,v,a}}; \qquad a=t-v
```
where $\boldsymbol{r}$: apparent [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)-key survival rate, $\boldsymbol{p}$: Report A class (PASS or COMM), $\boldsymbol{k}$: abbreviated [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make-model key, $\boldsymbol{v}$: vintage of that key, $\boldsymbol{a}$: vehicle age, $\boldsymbol{t}$: Report A edition year, and $\boldsymbol{N_{p,k,v,a}}$: observed FIT_ACTIVE stock.

```math
(ii)\quad R^{\mathrm{MTO}}_{c,a}=1 - \frac{\sum_v D_{c,v,a}}{\sum_v E_{c,v,a}}; \qquad E_{c,v,a}=\sum_{p,k → m(k,v)=c}N_{p,k,v,v+a}; \qquad D_{c,v,a}=\sum_{p,k → m(k,v)=c}\left(N_{p,k,v,v+a}-N_{p,k,v,v+a+1}\right)
```
where $\boldsymbol{R}$: aggregated vehicle class survival rate, $\boldsymbol{c}$: target vehicle class, $\boldsymbol{m(k,v)}$ reviewed class assigned to a particular [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)-key and vintage, $\boldsymbol{E}$: pooled starting exposure—the sum of beginning-of-transition stock contributing to a class-vintage-age estimate, $\boldsymbol{D}$: aggregated apparent retirements.

```math
S_c(a+1)=S_c(a)R_{c,a}; \qquad S_c(0)=1
```
The cumulative [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) survival $\boldsymbol{S_c(a)}$ is a product of those empirical rates with an explicit age-zero baseline, following the [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) indexing method. Because no vehicle-level identifier links editions, **apparent retirements mix physical retirement with administrative status changes, migration, imports, re-registration, and [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)-key changes**.

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Standardize source survival schedules | Cars and trucks | Keep [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) cumulative values. Convert [*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) scrappage and [*EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) annual retention to cumulative survival from one at age zero. [*EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) rate at age a supplies survival at a+1, including rate40 to age41. #to-review |
| Map source schedules to model classes | Cars and trucks | [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/)/[*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) uses [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) cars, weighted light trucks, [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) 2b/3 medium trucks and [*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) heavy trucks. [*EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) uses car/light/heavy schedules, with medium trucks sharing the heavy schedule. Apply configured class weights. #to-review |
| Compile fixed lifetimes | All | With curves off, selected car/truck classes use the first age at which source survival reaches 50% or less. Remaining modes retain [*StatCan*](https://www.statcan.gc.ca/en/developers/wds) or reviewed fixed lifetimes. #to-review |
| Select the lifetime representation | All | Curves off uses fixed lifetimes. Curves on uses annual survival and a matching process lifetime; Temoa computes period averages. #to-review |
| Reconcile terminal survival | Cars and trucks | Retain native values, extend the last annual retirement rate to 0.5% survival, then append zero. Apply an optional hard zero age from the scenario. Use this schedule for calibration and future survival; preserve annual totals when grouping vintages and audit any adjustment required by Temoa. #to-review |
| Estimate [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) annual changes before mapping | Cars and light trucks | Compare `FIT_ACTIVE` counts for the same make-model and model year in consecutive Report A editions. Keep increases as observed rather than forcing every annual ratio between zero and one. |
| Map and aggregate [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) diagnostics | Cars and light trucks | Attach reviewed vintage-range mappings only after the raw annual changes are calculated. Sum the starting and following counts by [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) and [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) class and age before calculating class-level rates. |
| Limit and check [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) diagnostic evidence | Cars and light trucks | Use starting ages 0 through 35, including eligible pre-2000 model years. Report mapping coverage, age gaps, unusual rates, and sample support; do not fill missing ages or the older tail with [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) or [*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) values. |
| Keep [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) results diagnostic | Cars and light trucks | Treat the results as changes in registered stock, not direct observations of vehicle retirement. Compare them with accepted [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) curves only after the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) series is derived; production uses the configured [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/)/[*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) or [*EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) profile. #to-review |

**Notes:**

- **Execution boundary.** The default `parameterization.road_lifetimes_survival` command
  publishes accepted source-based curves and source-derived median lifetimes without
  loading [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) history or its reviewed mapping. `--mto-diagnostics` publishes the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)
  review evidence; `--all` runs both paths.
- Detailed [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) filters, evidence checks, and comparisons are documented in
  `docs/insights/vehicle_population_diagnosis.py`.

### `efficiency`

!!! note "Keep consumption weighting and fuel boundaries explicit"
    Average physical consumption before inversion. The [energy method](model/energy.md) explains PHEV components, trajectory indexing and optional [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) calibration. #to-review

*Note: Some technologies are not represented in this diagram; see
`config/parameters/rules.yaml` for implemented harmonization entries.*

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  subgraph road_agg["`**Road vehicle class aggregation**`"]
    subgraph agg["`**Road vehicle population evidence**`"]
      s3@{shape: doc, label: "**Wards Intelligence**
        National LDV and MHDV sales from 2021 by make-model, and GVWR class; covers aggregation in provinces without detailed vehicle population data"}
      s4[("`**ON Ministry of Transport (MTO)**
        • *Report A:* vehicle counts by make-model codes, model year, and status
        • *Report 4:* counts by gross weight bucket, (COMM, PASS, BUS), and status`")]
      s10[("`**StatCan Tables**
        Truck shipment distance and tonne-km where province is origin or destination`")]
      s5[("`**Quebec SAAQ #to-do**
        Active vehicle counts by make-model, vintage, and inferred size class`")]
    end

    p0["`**Road aggregation mapping**
      • *LDVs:* map vehicle size classes and derive efficiency aggregation weights
      *MTO Report A is mapped into NRCan and ATB classes via MTO make-model similarity*<br>
      • *MD/HD trucks:* map truck weight classes and derive efficiency aggregation weights
      *Report 4 distributes medium truck gross weight class cohorts*<br>
      • *HD trucks:* derive regional- and long-haul activity weights`"]
    s3 -- inputs/0_manual_params/ --> p0
    s4 -- vehicle_population.py --> p0
    s10 -- statcan_tables.py --> p0
  end
    p0_2@{shape: notch-rect, label: "**Vehicle population mapping diagnosis**
      MTO make-model codes are difficult to map. This notebook visualizes:<br>
      • mapped fit-active stock
      • what cannot be mapped reliably
      • vehicle class and vintage weights
      • Report A vs Wards LDV class shares
      • Report 4 vs Wards MD truck shares
      • survival rates from mapped cohorts"}
    p0 -- vehicle_population_diagnosis.py --> p0_2

  subgraph road["`**Road efficiencies**`"]
    s1[("`**NRCan Fuel Consum. Ratings**
      Car and light-truck ratings by make and model`")]
    s2[("`**Autonomie TEA via NLR ATB**
      Future vehicle efficiencies and powertrain multipliers by scenario
      ***Def. scenario:*** mid trajectory`")]
    s2_2[("`**JGCRI GCAM model**
      Motorcycle (>250 cc) efficiencies for Canada`")]
  end
  
  subgraph off["`**Off-road efficiencies**`"]
    direction LR
    s7@{shape: docs, label: "**EPRI REGEN model assumptions**
      Future multipliers for inter-city buses and off-road modes"}
    s8@{shape: doc, label: "**EIA NEMS model assumptions**
      Fuel consumption improvement of -1%/year for new jet aircraft"}
  end
  
  s0[("`**NRCan CEUD**
    Truck fleet avg. fuel consumption in L/100 km; off-road energy intensity in MJ/service unit`")]
  s9[("`**NRCan CEUD**
    Activity, avg. annual distance travelled and stock used to derive occupancy and payload`")]
  s11@{shape: processes, label: "**Annual cohort aggregation weights**
      Use physical surviving cohort counts by fuel and class; never acquisition-capacity weights #to-review"}

  %% ############ Processes ###########
  p2_2@{shape: hex, label: "**config/scenarios/**
    *atb_scenario:* mid, conservative, or advanced"}
  p2["`**Road consumption and projection anchors**
    • *Existing LDVs:* retain each powertrain/range's own aggregated NRCan fuel consumption<br>
    • *Existing MD/HD trucks:* use own CEUD consumption; emerging technologies use diesel × ATB consumption ratios<br>
    • *New LDVs:* project latest cohort anchors with ATB ratios; BEV/PHEV ranges use BEV300/PHEV35 anchors<br>
    • *New MD/HD trucks:* apply aggregated ATB consumption ratios to cohort anchors; gasoline uses diesel's ratio<br>
  `"]
  p0 -- road_aggregation.py --> p2
  s1 -- nrcan_ceud.py --> p2
  s2 -- nlr_atb_autonomie.py --> p2_2 --> p2
  s2_2 -- assorted_sources.py --> p2
  s11 -- build_existing_capacity.py --> p4

  p2_3@{shape: notch-rect, label: "**LDV efficiency-trajectory diagnosis**
      Bridging NRCan Ratings baseline with trajectories of ATB improvement rates. This notebook visualizes:<br>
      • distribution of NRCan Ratings evidence 
      • OMEGA-derived PEV range composition
      • historical NRCan efficiency trajectories
      • projected NLR ATB efficiency trajectories
      • overlapping of harmonized trajectories 
      • indexed production trajectories 
      "}
    p2 -- ldv_efficiency_diagnosis.py --> p2_3
  
  s0 -- nrcan_ceud.py --> p3 & p2

  p3["`**Off-road baseline and indexing**
    • *Existing off-road modes:* use incumbent fleet energy intensity<br>
    • *New off-road modes:* index existing efficiencies to future multipliers`"]
  off -- inputs/0_manual_params/ --> p3
    
  p4["`**Period aggregation & unit conversion**
    • Weight car/truck consumption and load by vintage-powertrain cohort counts<br>
    • Apply configured stock proxies; report observed coverage<br>
    • Convert aggregated consumption to reciprocal service-output efficiency<br>
  `"]
  s9 -- nrcan_ceud.py --> p4
  p2 -- road_efficiencies.py --> p4
  p3 -- offroad_efficiencies.py --> p4
  p4 -- build_efficiencies.py --> o1[/"`***efficiency***
    [bn passenger-km/PJ]
    [bn tonne-km/PJ]`"/]
  
  %% --- Hyperlinks ---
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

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Map size classes and derive aggregation weights | Cars and light trucks | - Map [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) make-model-vintage keys from normalized [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64), [*US FuelEconomy.gov*](https://www.fueleconomy.gov/feg/ws/index.shtml), and [*NHTSA vPIC*](https://vpic.nhtsa.dot.gov/api/) API evidence<br>- Derive latest-snapshot fleet composition weights by mapped vehicle class |
| Map weight classes and derive aggregation weights | MD/HD trucks | Map truck weight-rating counts to classes that align with [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) truck projection classes |
| Derive regional- and long-haul activity weights | Heavy-duty trucks | Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) haul classes |
| Aggregate NRCan ratings using mappings | Cars and light trucks | Within each exact model-year, size-class and powertrain/range cohort, average distinct make-models (or use the configured median), then apply [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) class weights. [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) supplies class/vintage evidence, not powertrain/range shares. |
| Retain each range's observed consumption | Cars and light trucks | Retain each range's exact NRCan history. Form PHEV totals from CD electricity/liquid fuel and CS fuel using the matched [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) bucket's utility factor. Missing years reduce coverage; only wholly empty bins may use a recorded, configured analogue. |
| Anchor future LDVs to Canadian cohorts | Cars and light trucks | Weight the latest historical period bin's consumption by physical survivor counts, independent of solver-capacity representation. #to-review Use each non-PEV's own [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) improvement ratio; BEV/PHEV ranges use BEV300/PHEV35 anchors: `C_range(y) = C_anchor × ATB_range(y) / ATB_anchor(reference)`. Use observed years in the latest configured historical bin, through `base_year`, to establish the anchor; use the configured [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) bridge reference year. #to-review |
| Derive emerging truck history | MD/HD trucks | Multiply annual [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) diesel consumption by the aggregated [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) target/diesel consumption ratio at the first projection reference year. All truck histories use average fleet consumption as vintage proxies; emerging histories (e.g., battery trucks) are derived, not observed model-year efficiencies. |
| Project from the latest truck cohort | MD/HD trucks | Weight the latest period bin's incumbent or derived emerging consumption by its own cohort counts. Use an explicit diesel cohort proxy when an emerging powertrain has no positive own counts. Apply each powertrain's [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) future/reference consumption ratio (diesel's for gasoline); hold the load factor averaged with the same cohort weights. |
| Special handling of buses | Transit, school, intercity | Existing school/transit efficiencies are indexed backwards from [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) using [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) historical service-intensity ratios. Intercity buses use an [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) transit diesel baseline with [*EPRI REGEN*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) powertrain/diesel consumption ratios; HEVs retain the [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) transit HEV/diesel ratio. |
| Special handling of motorcycles | Motorcycles | Future efficiencies retain the [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) baseline and apply [*JGCRI GCAM*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) consumption ratios from `UCD_trn_data_CORE.csv`; where motorcycle `engine > 250 cc`. |
| Prepare period consumptions | All | Through `base_year`, use the [existing-period year bins](#existing-period-handling). Average car/truck consumption per distance and occupancy/payload with cohort counts before lifetime filtering/redistribution; renormalize over observed years and report covered stock. Normalize each existing LDV PEV range over its supported years, then apply the complete [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) share vector. Weight annual bus, motorcycle and off-road consumption/intensity equally; derive future consumption at interval-end years. |
| Convert to service-output efficiency units | All | Finally, divide aggregated occupancy/payload by aggregated energy per vehicle-km. For [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) energy-per-service inputs, invert the aggregated intensity once. Convert fuel economy and relative efficiency factors to consumption before indexing or averaging, using configured HHVs and units. |

#### Notes

- Annual PHEV blend technologies use `limit_tech_input_split_annual` for their utility-weighted annual average fuel/electricity consumption split.
!!! note "PHEV architecture changes the comparison basis"
    Current [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) parameters use Voltec EREV PHEVs. The retained legacy comparison uses parallel PHEVs from [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/); their parameter differences include that architecture choice. #to-review
- Demand from other off-road modes includes vehicles not registered for on−road travel such as ATVs, snowmobiles, golf carts and some military vehicles, as per [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) notes; assumed to consume diesel only, passed directly as units of PJ for complete secondary-energy accounting.

#### Vehicle make-model to vehicle classes mapping

The [canonical vehicle reference](../config/parameters/vehicle_reference.csv) owns
year-scoped vehicle identities and classes. The [source-link table](../config/parameters/vehicle_source_links.csv)
owns native labels and their supporting evidence. Mapping uses those reviewed
scopes rather than model-name-only examples. #to-review

### `cost_invest`

!!! warning "Diagram limitation: price factors differ by vehicle family"
    Parameterization divides [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data)
    retail prices by 1.5 for LDVs and 1.2 for MHDVs; retail MSRP is retained
    separately for the LDV repair equation. #to-review
    The diagram's common 1.5 label omits this distinction. #to-fix: the MHDV
    markup label must show 1.2 to describe current investment costs accurately.

*Note: Some technologies are not represented in this diagram; see
`config/parameters/rules.yaml` for implemented harmonization entries.*

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s3[("`**CER Canada's Energy Future**
    Currency exchange rates and GDP deflator by scenario
    ***Def. scenario:*** current measures`")]
  subgraph offroad["`**Off-road CAPEX**`"]
    s7@{shape: docs, label: "**SFU CIMS model assumptions**
      Capital cost allocation of new off-road transportation and service output for demand-unit normalization"}
    s8@{shape: docs, label: "**EPRI REGEN model assumptions**
      CAPEX multipliers of alternative off-road modes"}
    s9@{shape: docs, label: "**FAA Benefit-Cost Analysis**
      • *Table 3-6 & 3-9:* Average block speeds, aircraft capacities, and load factors<br>
      • *Table 3-7 & 3-10:* Avg. daily utilization"}
  end

  %% ############ Processes ###########
  p3["`**Cost of new off-road demand**
    • Capital cost of building new transport capacity to satisfy off-road demand *[dollars/demand unit]*<br>
    • *Aircraft:* CAPEX normalized with utilization and load factors from FAA`"]
  s7 & s8 -- inputs/0_manual_params/ --> p3
  s9 -- assorted_sources.py --> p3

  %% ############ Sources ###########
  subgraph road["`**Road vehicle costs**`"]
    s1@{shape: processes, label: "**Road aggregation maps**
      Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
    s2[("`**Autonomie TEA via NLR ATB**
      Modeled vehicle price by class and powertrain by scenario
      ***Def. scenario:*** mid trajectory`")]
    s8_bus@{shape: doc, label: "**EPRI REGEN model assumptions**
      Vehicle price projections of intercity buses"}
  end

  %% ############ Processes ###########
  p0_2@{shape: hex, label: "**config/scenarios/**
    *atb_scenario:* mid, conservative, advanced"}
  p2["`**Vehicle manufacturing costs**
    • Revert vehicle prices back to manufacturing costs, divide by the RPE markup factor of 1.5<br>
    • Aggregate manufacturing cost projections by vehicle class using efficiency mappings`"]
  s2 -- nlr_atb_autonomie.py --> p0_2 --> p2
  s1 -- road_aggregation.py --> p2
  s8_bus -- assorted_sources.py --> p2

  p0@{shape: hex, label: "**config/scenarios/**
    *cer_scenario:* current, higher, lower, or net-zero"}
  p4["`**Harmonize currency units**
    • Apply exchange rate to CAD
    (e.g., 2023USD → 2023CAD)<br>
    • Discount to reference year
    (e.g., 2023CAD → 2020CAD)<br>
    • Harmonize magnitude of denominators`"]
  p2 -- road_capex_opex.py --> p4
  p3 -- offroad_capex_opex.py --> p4
  s3 -- cer_enerfuture.py --> p0 --> p4

  p4 -- build_costs.py --> o1[/"`***cost_invest***
    [$M 2020CAD/k vehicles]
    [$M 2020CAD/bn passenger-km]
    [$M 2020CAD/bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s8_bus "https://us-regen-docs.epri.com/v2025/assumptions/transportation.html#on-road-fleet-vehicles"
  click s8 "https://us-regen-docs.epri.com/v2025/assumptions/transportation.html#non-road-vehicles"
  click s7 "https://github.com/EMRG-SFU/cims-models/tree/main/sources/sectors"
  click s2 "https://vms.taps.anl.gov/research-highlights/vehicle-technologies/u-s-doe-vto-hfto-r-d-benefits/"
  click s3 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
  click s9 "https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost"
```

**Cost per block-hour** is maintenance cost per aircraft operating hour;
**Block speed** is distance per hour; **Seats** or **Tonnes** is carrying
capacity; and **Load factor** is the occupied fraction. The quotient is cost
per passenger-distance or tonne-distance, before unit harmonization. #to-review

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps | - Cars and light trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) and [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) projections<br>- Map truck weight-rating counts to classes that align with [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) haul classes |
| Revert vehicle prices back into manufacturing costs | Road vehicles | Current [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) treatment divides retail prices by RPE 1.5 for LDVs and 1.2 for MHDVs; the resulting manufacturing-cost proxy retains its source and dollar-year evidence. #to-review |
| Aggregate manufacturing costs using mappings | Cars and trucks | Use mapped weights to aggregate [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) projected manufacturing costs of cars, LD, MD, and HD trucks, and school and transit buses |
| CAPEX of new off-road demand capacity | Off-road modes | Obtain input capital costs of building new supply capacity that satisfies off-road demand, derived as dollars per demand unit |
| Special handling of buses | Intercity buses | Use [*EPRI REGEN*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) vehicle purchase cost assumptions for intercity buses |
| Special handling of motorcycles | Motorcycles | Use [*JGCRI GCAM*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) purchase costs |
| Harmonize currency units | All | Convert values using foreign currencies and/or different dollar years into reference currency-year |

### `cost_variable`

!!! warning "Use the empirical relationship within its stated limits"
    The [technology-cost chapter](model/technology.md#maintenance-and-repair) explains repair evidence, age handling and price-basis limitations. Convert vehicle-distance costs using the same occupancy/payload as efficiency. #to-review

*Note: Some technologies are not represented in this diagram; see
`config/parameters/rules.yaml` for implemented harmonization entries.*

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  subgraph offroad["`**Off-road OPEX**`"]
    s7@{shape: docs, label: "**CMU OEO model assumptions**
      Variable costs of rail techs set to 6% (freight) and 10% (passenger) of CAPEX; marine freight set to 5%"}
    s8@{shape: docs, label: "**FAA Benefit-Cost Analysis**
      • *Table 4-7 & 4-8:* Passenger & cargo aircraft avg. maintenance costs/block-hour<br>
      • *Table 3-6 & 3-9:* Average block speeds, aircraft capacities, and load factors"}
  end

  %% ############ Processes ###########
  p3["`**Variable costs from off-road**
    • *Aircraft:* **eq. (i-ii)** normalized maintenance costs per demand unit<br>
    • *Other off-road:* estimate variable costs with OEO ratios`"]
  s7 -- inputs/0_manual_params/ --> p3
  s8 -- assorted_sources.py --> p3

  %% ############ Sources ###########
  subgraph road["`**Road M&R costs**`"]
    s1@{shape: processes, label: "**Road aggregation maps**
      Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
    s2[("`**NLR ATB (Burnham et al. 2021)**
      Avg. maintenance costs per mile, size and powertrain multipliers, and repair cost coefficients for LDVs<br>
      **NLR ATB (Autonomie TEA)**
      Modeled vehicle price by class and powertrain by scenario
      ***Def. scenario:*** mid trajectory`")]
    s3@{shape: win-pane, label: "**ANL BEAN (Islam et al. 2022)**
      Maintenance and repair linear model coefficients for MHDVs"}
  end

  %% ############ Processes ###########
  p0_2@{shape: hex, label: "**config/scenarios/**
    *atb_scenario:* mid, conservative, or advanced"}
  p2["`**Maintenance & repair costs**
    • *LDVs:* **eq. (i)** get age-dependent repair cost via empirical model;
    **eq. (ii)** add avg. maintenance costs per mile (Burnham et al. 2021)<br>
    • *MHDVs:* **eq. (iii)** age-dependent M&R costs via empirical model (Islam et al. 2022)<br>
    • Aggregate M&R cost-per-mile curves by vehicle class using efficiency mappings`"]
  s2 -- nlr_atb_autonomie.py --> p0_2 --> p2
  s3 -- inputs/0_external_models/ --> p2
  s1 -- road_aggregation.py --> p2

  s0[("`**CER Canada's Energy Future**
    Currency exchange rates and GDP deflator by scenario
    ***Def. scenario:*** current measures`")]
  s9[("`**NRCan CEUD**
    Vehicle/mode occupancy and payload factors`")]

  p0@{shape: hex, label: "**config/scenarios/**
    *cer_scenario:* current, higher, lower, or net-zero"}
  p4["`**Harmonize currency units**
    • Apply exchange rate to CAD
    (e.g., 2023USD → 2023CAD)<br>
    • Discount to reference year
    (e.g., 2023CAD → 2020CAD)<br>
    • Harmonize magnitude and units of denominators`"]
  p2 -- road_capex_opex.py --> p4
  p3 -- offroad_capex_opex.py --> p4
  s0 -- cer_enerfuture.py --> p0 --> p4
  s9 -- nrcan_ceud.py --> p4

  p4 -- build_costs.py --> o1[/"`***cost_variable***
    [$M 2020CAD/k vehicles]
    [$M 2020CAD/bn passenger-km]
    [$M 2020CAD/bn tonne-km]`"/]

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

In these relationships, **Repair** and **Maint.** are repair and scheduled
maintenance per vehicle-mile; **age** is years; **size** and **pwt** are
dimensionless size/powertrain multipliers; **C** is the source age coefficient;
**price** is retail MSRP on its registered currency/year basis; $\boldsymbol{\beta}$
has reciprocal-currency units; **m** is the medium/heavy-truck slope in source
currency per mile per year; and **b** is its intercept in source currency per mile. Detailed coefficients
and transfer limitations are retained in [cost assumptions](assumptions.md#cost_variable). #to-review

#### Off-road M&R equations

```math
(i)\; \mathrm{M\&R}^\mathrm{Air\;travel}=\frac{\mathrm{Cost\;per\;block\text{-}hour}}{\mathrm{Block\;speed}\cdot \mathrm{Seats}\cdot \mathrm{Load\;factor}}
```

```math
(ii)\; \mathrm{M\&R}^\mathrm{Air\;freight}=\frac{\mathrm{Cost\;per\;block\text{-}hour}}{\mathrm{Block\;speed}\cdot \mathrm{Tonnes}\cdot \mathrm{Load\;factor}}
```

**Cost per block-hour** is maintenance cost per aircraft operating hour;
**Block speed** is distance per hour; **Seats** or **Tonnes** is carrying
capacity; and **Load factor** is the occupied fraction. The quotient is cost
per passenger-distance or tonne-distance, before unit harmonization. #to-review

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps | - Cars and light trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) and [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) projections<br>- Map truck weight-rating counts to classes that align with [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) haul classes |
| Vehicle prices used in repair cost empirical model | LDVs | Use the registered retail MSRP in the empirical repair relationship, retaining its source-year/currency basis. The investment-cost RPE divisor does not modify this input. #to-review |
| Aggregate M&R costs using mappings | Cars and trucks | Use mapped weights to aggregate M&R cost curves of cars, LD, MD, and HD trucks, and transit buses |
| Variable costs from off-road | Off-road modes | - Estimate annual variable costs of rail and marine vessels with CAPEX-to-OPEX ratios used in the [*OEO*](https://github.com/TemoaProject/oeo/tree/master/database_documentation) model<br>- Estimate M&R costs per demand unit with empirical data from the [*FAA Economic Values*](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost), converting miles to km and tons to tonnes; same annual utilization and load factors are used for CAPEX and OPEX |
| Special handling of buses | School and intercity buses | Assume same CAPEX-to-OPEX ratios as those from transit buses |
| Special handling of motorcycles | Motorcycles | Use [*JGCRI GCAM*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) M&R costs |
| Harmonize currency units | All | - Convert values using foreign currencies and/or different dollar years into reference currency-year<br>- Harmonize denominators; convert miles into km and multiply by [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) load factors |

#### Notes

[*FAA Economic Values*](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) source PDFs: [Section 3 — Aircraft Capacity and Utilization Factors](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost/econ-value-section-3-capacity.pdf) and [Section 4 — Aircraft Operating Costs](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost/econ-value-section-4-op-costs.pdf).

### `emission_embodied`

!!! note "Preserve lifetime vehicle-cycle totals"
    These factors apply to new manufacture, with separate gas units and a defined vehicle-cycle scope. They are distinct from fuel-cycle and operating emissions. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  subgraph greet["`**Argonne National Lab GREET model**`"]
    direction TB
    s1@{shape: win-pane, label: "**GREET_1 (fuel-cycle) and GREET_2 (vehicle-cycle) Excel models**
      Solved with default inputs for model year 2025, can vary through 2050 if *xlwings* is configured to do so. Default classes are:<br>
      • LDVs: Cars, SUVs, and pickup trucks
      • MHDVs: Class 6 and Class 8 Day/Sleeper"}
    s3@{shape: docs, label: "**Solved GREET model runs**
      Vehicle-cycle lifetime emissions by scenario
      *Def. scenario*: conventional materials"}
    s1 -- "`solved via *xlwings* library`" --> s3
  end

  s2@{shape: processes, label: "**Road aggregation maps**
    Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}

  %% ############ Processes ###########
  p1@{shape: hex, label: "**config/scenarios/**
    *embodied_emissions:* true or false
    *embodied_materials*: conventional or lightweight"}
  greet -. inputs/0_external_models/ .-> p1

  p2["`**Vehicle manufacturing emissions**
    • Normalize units as k tonnes/k vehicles, including CO<sub>2</sub>, CH<sub>4</sub>, and N<sub>2</sub>O factors<br>
    • Aggregate light, medium, and heavy truck classes using efficiency mappings`"]
  p1 -. true .-> p2
  s2 -- road_aggregation.py --> p2

  p2 -. "road_embodied_emissions.py" .-> o1[/"`**emission_embodied**
    [k tonnes/k vehicles]`"/]

  %% --- Hyperlinks ---
  click s1 "https://greet.anl.gov/"
```

**Cost per block-hour** is maintenance cost per aircraft operating hour;
**Block speed** is distance per hour; **Seats** or **Tonnes** is carrying
capacity; and **Load factor** is the occupied fraction. The quotient is cost
per passenger-distance or tonne-distance, before unit harmonization. #to-review

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps | - Cars and LD trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) and [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) projections<br>- Map truck weight-rating counts to classes that align with [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate [*ANL Autonomie*](https://vms.taps.anl.gov/tools/autonomie/) haul classes |
| Vehicle manufacturing emissions | Cars and trucks | - Default vehicle-lifetime emissions from [*ANL GREET*](https://greet.anl.gov/greet/versions.html)-2 are extracted directly, solved for model year 2025 and normalized by k tonnes per thousand vehicles manufactured; model years 2030 through 2050 remain available.<br>- [*ANL GREET*](https://greet.anl.gov/greet/versions.html) 2 calculates the emissions associated with the production and processing of vehicle materials, the manufacturing and assembly of the vehicle, and the EOL decomissioning. EOL credits from material recycling are not considered. Emissions from the transportation of raw and processed materials for each process step are neglected. |

### EV charger parameters

!!! warning "Port count, rated power and throughput are different quantities"
    Use expenditure divided by rated power for charger costs. The [infrastructure chapter](model/infrastructure.md) defines utilization, loss accounting and the different hydrogen-refueling capacity basis. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s1@{shape: doc, label: "**NRCan/Dunsky 2024 EV Charging Infrastructure Assessment**
    • *Annual LD UFs:* Table 31; average LDV charging port utilization rate<br>
    • *EV-to-port ratios*: Tables 7 and 37; 1 LDEV: 1 LD charger, including residential ports, and 1.5 MHDEV: 1 MHD charger<br>
    • *MHD chargers:* Table 19 assumptions for power levels, charging time, and daily throughput<br>
    • *LDV and MHDV charger CAPEX:* Tables 12 and 21; per-port installation and equipment cost estimates<br>
    • *LDV and MHDV charger type shares:* Tables 6 and 38; estimated charging infrastructure needs, used only to aggregate capital and fixed costs"}
  s2[("`**Transport Canada (TC) EV Dashboard**
    • *Public LD chargers by type:* Cumulative number of public L2 and DCFC chargers<br>
    • *Public LD chargers by province:* Latest aggregated total public LD charger count`")]
  s3@{shape: doc, label: "**Pollution Probe 2024 Charging Experience Survey**
    Home charging access to L1 and L2 chargers, based on responses from ~2,000 representative EV owners; 84% and 15% of owners had L2 and L1 chargers, respectively"}
  s4@{shape: doc, label: "**NLR 2024 Assessment of Alternative Fueling Infrastructure**
    L1 residential charger installation cost per port; excluding charger cost since these tend to be included with PEV acquisition"}
  s5@{shape: docs, label: "**CMU OEO model assumptions**
      Fixed costs of charging infrastructure set to 1% of CAPEX (Borlaug et al. 2021)"}
  oi[/"`***Physical surviving stock***
    [k vehicles]
    *Retrieve LD and MHD EV survivors at base_year; not acquisition capacity* #to-review`"/]

  %% ############ Processes ###########
  p0["`**Aggregated capital and fixed costs**
    Match per-port CAPEX and rated kW at each cost source year; convert money to CAD2020.<br>
    • *Capital rate*: Σ(count share × cost/port) / Σ(count share × rated kW)<br>
    • *Units*: CAD/kW = million CAD/GW<br>
    • *Annual fixed O&M*: 1% of capital rate during the vintage's lifetime<br>
    Prospective vintage 2025 uses source 2030; existing costs use the first model year. #to-review`"]
  p0_2@{shape: hex, label: "**inputs/0_manual_params/**
    Native per-port CAPEX, power trajectories and port-count shares:
    • *(L1, L2, DCFC)* = [0.12, 0.87, 0.01]
    • *(50, 350, 2000) kW* = [0.82, 0.16, 0.02]<br>
    Derive power shares; hold final source values through 2050. #to-review"}
  s4 & s5 -- inputs/0_manual_params/ --> p0
  s1 --> p0_2 -- ev_chargers.py --> p0
  p0 -- ev_chargers.py --> o0 & o0_2
  o0[/"`***cost_invest***
    [$M 2020CAD/GW]`"/]
  o0_2[/"`***cost_fixed***
    [$M 2020CAD/GW]`"/]

  p1["`**Annual EV charger utilization rate**
    • *All chargers*: reviewed mean UF = 0.20 for existing/new vintages<br>
    • *Profiled LDV*: scale hourly ceilings to mean UF; annual floor = UF × 0.98<br>
    • *Other chargers*: annual equality to UF, without slack. #to-review`"]
  p1_2@{shape: hex, label: "**inputs/0_manual_params/**
    • *LDV and MHDV UF*: common 0.20<br>
    Reviewed override for the vintage-indexed annual limit. #to-review"}
  s1 --> p1_2 -- ev_chargers.py --> p1
  p1 -- ev_chargers.py --> o1[/"`***limit_annual_capacity_factor***
    *Indexed by capacity vintage*<br>
    *LDV with profile: ≥ 0.196*
    *LDV w/o profile: = 0.20*
    *MHDV: = 0.20* #to-review`"/]

  p2["`**Existing charging infrastructure capacity**
    • Fetch TC total public LD chargers by province and distribute by type using national shares<br>
    • EV-to-charger port ratios determine total ports, from which public port capacity is known<br>
    • Thus, the remainder LD charger capacity is private, distributed by L1/L2 shares from Pollution Probe survey<br>
    • MHD charger capacity derived with EV-to-port ratio and distributed with Dunsky's share estimates<br>
    Convert ports to GW with first-model-year registered ratings; 2025 LDV DCFC is 125 kW. #to-review`"]
  p2_2@{shape: hex, label: "**config/scenarios/**
    • *LDEV_to_port_ratio* = 1
    • *MHDEV_to_port_ratio* = 1.5"}
  s1 --> p2_2 -- ev_chargers.py --> p2
  s1 -- inputs/0_manual_params/ --> p2
  s2 -- assorted_sources.py --> p2
  s3 -- inputs/0_manual_params/ --> p2
  oi -- ev_chargers.py --> p2
  p2 -- ev_chargers.py --> o2[/"`***existing_capacity***
    [GW]`"/]

  %% --- Hyperlinks ---
  click s1 "https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada#a34"
  click s2 "https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard"
  click s3 "https://www.pollutionprobe.org/pollution-probe-2024-canadian-electric-vehicle-owner-charging-experience-survey-report/"
  click s4 "https://docs.nlr.gov/docs/fy24osti/88513.pdf"
  click s5 "https://github.com/TemoaProject/oeo/blob/master/database_documentation/TransportationSector.ipynb"
```

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Aggregated capital and fixed costs | LDV and MHDV chargers | Match native per-port costs and ratings by source year; convert currency, then divide total port expenditure by total rated power. Derive power shares from count shares × kW. Fixed O&M is 1% annually within the vintage lifetime. #to-review |
| Annual EV charger UF | LDV and MHDV chargers | Reviewed common UF = 0.20 for every existing/new vintage. Profiled LDV ceilings have mean 0.20 and annual floor 0.196; no-profile LDV and MHDV use equality 0.20. #to-review |
| Existing charging capacity | LDV and MHDV chargers | Preserve TC public counts, reviewed EV-to-port ratios and private/type mixes. Convert ports with first-model-year registered power trajectories; audit the selected source year and ratings. #to-review |
| Charger efficiency | LDV and MHDV chargers | Unity accounting transfer avoids repeating losses embedded in vehicle consumption. Reviewed ICCT factors remain unapplied; motorcycle losses are omitted, and [*EPRI REGEN*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html)'s intercity-bus boundary is undisclosed. #to-review |

### `limit_new_capacity_share` for LD-PEV range composition

!!! note "Within-family shares leave total technology adoption open"
    The [range method](model/technology.md#range-is-a-representation-choice) explains the evidence and optimizer choices. Frozen charging composition is not reweighted by the new-vehicle shares. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  s1@{shape: win-pane, label: "**EPA OMEGA v2.5 LDV central baseline**
    Registered MY2022 BEV/PHEV extract:<br>
    • Baseline PEV sales and charge-depleting ranges<br>
    • US car and light-truck market classes"}
  s2@{shape: processes, label: "**Road aggregation maps**
    Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}

  %% ############ Processes ###########
  p2["`**PEV range buckets and market shares**
    • Assign configured charge-depletion range buckets<br>
    • Sum sales by vehicle class, powertrain, and CD range<br>
    • Reject unresolved sales; retain the full denominator`"]
  s1 -- epa_omega_baseline.py --> p2
  s2 -- road_aggregation.py --> p2

  p0@{shape: hex, label: "**config/scenarios/**
    *BEV_PHEV_range_representation:*
    new_capacity_shares or representative_archetype"}
  p2 -- "ldv_ev_ranges.py" --> p0

  p3["`**Minimum new-capacity shares**
    • Map US class shares to Canadian LDV categories<br>
    • Group each range variant within its new-build class/powertrain family<br>
    • Repeat shares across configured regions and model vintages`"]
  p0 -. new_capacity_shares .-> p3

  p3 -. "ldv_ev_ranges.py" .-> o1[/"`***limit_new_capacity_share***
    [-]
    *Operator: ≥*`"/]
  p3 -. "ldv_ev_ranges.py" .-> o2[/"`***tech_group, tech_group_member***
    Backend-owned numerator and denominator membership`"/]

  oi2[/"`LD-PEV range family params, e.g.: 
    ***efficiency***
    ***cost_invest***
    ***cost_variable***`"/]
  p4["`**Replace LD-PEV range variants with market-share aggregates**
    • Use PEV range market shares<br>
    • Derive representative parameter aggregates<br>
    • Replace LD-PEV range archetypes and parameters with aggregate ones`"]
  oi2 -- ldv_ev_ranges.py --> p4
  p0 -. representative_archetype .-> p4

  o3[/"`Representative LD-PEV params, e.g.:
    ***efficiency***
    ***cost_invest***
    ***cost_variable***`"/]
  p4 -. "ldv_ev_ranges.py" .-> o3

  %% --- Hyperlinks ---
  click s1 "https://www.epa.gov/regulations-emissions-vehicles-and-engines/optimization-model-reducing-emissions-greenhouse-gases"
```

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| PEV range buckets | Light-duty BEVs and PHEVs | Get [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) baseline vehicle classes, charge-depleting (CD) miles, and modeled sales. Map regulatory `truck` to `light_truck`; use sales weights and CD-range buckets: BEV ≤150, (150, 250], (250, 350], and >350 miles; PHEV <35 and ≥35 miles. [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) use these same buckets. |
| Scenario range representation | Light-duty BEVs and PHEVs | `none` adds no new-vehicle range constraints; `representative_archetype` replaces new range families with representative aggregates; `new_capacity_shares` enforces fixed market-share constraint on PEV range compositions by class.  |
| Minimum new-capacity shares | Light-duty BEVs and PHEVs | Numerator: one range variant. Denominator: all range variants in the same category/powertrain. Validate complete, unambiguous template membership; emit `ge` (≥) shares for every configured region/model vintage, excluding existing stock. |
| Representative aggregates | Light-duty BEVs and PHEVs | Use supplied, validated parameter batches, without share constraints; require complete coverage, matching units, and identical utilization, fixed lifetimes, and range survival curves. Its PHEV horizon-mean energy split conserves total energy; component-fuel differences are audited. |
| Existing stock and charging composition | Existing LD BEVs/PHEVs | Existing representatives reuse [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) weights for range-specific parameters independently of the new-vehicle mode, preserving stock totals and cohort ages. Frozen charging profiles already contain fleet/range composition; do not multiply them by these shares again. |

### `capacity_factor_tech` for BEV charging profiles; pending refactor

The preserved diagram describes inherited simulation choices. Current ingestion
reads registered minute-resolution output, computes hourly means, and scales the
native 8,760-hour mean to the selected annual utilization; it does not rerun the
simulation or add a rolling filter. Final representative-period weights need a
separate mean-consistency correction. #to-review

Registered [*RAMP-mobility*](https://github.com/RAMP-project/RAMP-mobility) outputs provide the charging shapes. The native build reads the registered minute-resolution series and prepares hourly means; it does not generate new travel simulations. #to-review

```mermaid
---
config:
  layout: elk
  flowchart:
    nodeSpacing: 35
    rankSpacing: 50
    wrappingWidth: 250
---
flowchart LR
  %% ############ Sources ###########
  subgraph vehicle["`**Vehicle fleet characteristics; these inputs are actively fetched for other parameters**`"]
    direction LR
    s3[("`**StatCan table**
      Registered vehicle size class shares by province; regardless of powertrain type`")]
    s8[("`**Autonomie TEA via NLR ATB**
      Modeled battery capacity at beginning of life by BEV size class`")]
    s9@{shape: win-pane, label: "**EPA OMEGA LD Central Case outputs**
      Representative battery capacities and CD ranges by BEV size class based on modeled market composition"}
  end

  subgraph legacy["`**legacy_backend/; currently not fetched by canoe-transportation v2**`"]
    direction TB
    s0[("`**Renewables Ninja weather profiles**
      Population-weighted, annual temperature profiles by province from MERRA-2 global dataset.`")]
    direction LR
    subgraph trip_other["`**Other household travel surveys**`"]
      s1_2[("`**2016 Tomorrow Transportation Survey**
        Same survey parameters as the US NHTS; limited to the Ontario GGH region, excluding weekends. *2022 survey data is available upon request*.`")]
      s2[("`**Canadian Survey on Everyday Travel**
        Announced as part of the 2026 Census of Population; yet to be released. #to-do`")]
    end

    subgraph trip["`**Trip characteristics and behavior**`"]
      direction LR
      s1[("`**FHA National Household Travel Survey**
        • Trips by mode, purpose, distance, duration, and type of day<br>
        • 24-hour trip start/end times by day type<br>
        • Trips by vehicle/fuel type and who drove`")]
      p1["`**Travel behavior from a typical week; no seasonal variability**
        • *Curate trip data*: exclude outliers, unrelated datapoints, and unclear responses<br>
        • *Map trip purposes:* classify trip reasons into personal- and occupation-related<br>
        • *Avg. trip characteristics:* get avg. daily driven distance and trip distance and duration by weekday and purpose<br>
        • *Get trip start-time distributions:* group trips by weekday and occupation and get trips' relative frequency by hour<br>
        • *Get main trip-occurrence windows:* hours when workers, students, and inactive cohorts drive the most`"]
      s1 -- charging_profiles/ --> p1
    end

    subgraph driver["`**Driver occupation composition**`"]
      direction LR
      s4[("`**StatCan 2021 Census of Population**
        Share of population in the labour force by province; unspecified driving status`")]
      s5[("`**StatCan 2021 Census of Population**
        Share of population attending postsecondary school by province; unspecified driving status`")]
    end

    subgraph charger["`**Charger characteristics and availability**`"]
      direction LR
      s6@{shape: doc, label: "**ICCT 2022 Quebec Charging Infrastructure Assessment**
        Shares of EV owners with access to home and workplace charging, respectively"}
      s7@{shape: doc, label: "**NRCan/Dunsky 2024 EV Charging Infrastructure Assessment**
        Projected total LDV charging ports by type (L1, L2, and DCFC) through 2050"}
    end
  end

  %% ############ Processes ###########
  subgraph ramp["`**legacy_backend/; simulation runs done externally**`"]
    direction TB
    p2@{shape: win-pane, label: "**RAMP-mobility simulation model**
      Stochastic BEV fleet aggregation framework; default parameters are:<br>
      • Representative fleet size of 2,500 BEVs<br>
      • Stochastic variability of total daily distance (±30%), avg. trip speed (±30%), battery consumption (±10%)<br>
      • Probability of charging during parking as logistic p(SOC); p(<0.2)=1 and p(>0.8)=0<br>
      • Probability of mobility events during (p=1) and outside (p=1/7) main windows"}
    p3["`**Resample and index charging profiles**
      • Rolling average resamples annual 15-min charging demand time series to 8,760 h<br>
      • Time zones index referenced to America/Toronto (EDT)`"]
  end
  p2 -- charging_profiles/ --> p3
  legacy -- charging_profiles/ --> ramp
  vehicle -- spreadsheet_database/ --> ramp

  ramp -- ldv_charging_profiles.py --> p4["`**Scale to annual charger utilization**<br>
    Hourly CF = peak-normalized profile × UF / full-year hourly profile mean.<br>
    Use the ordinary mean of all 8,760 hours; retain hourly chargers/upstream electricity and annual charger-output balance.`"]
  p4 --> o1[/"`***capacity_factor_tech***
    [-]`"/]

  %% --- Hyperlinks ---
  click s0 "https://www.renewables.ninja/"
  click s1 "https://nhts.ornl.gov/"
  click s2 "https://www23.statcan.gc.ca/imdb/p2SV.pl?Function=getSurvey&Id=1583347"
  click s3 "https://doi.org/10.25318/2010002501-eng"
  click s4 "https://doi.org/10.25318/9810048501-eng"
  click s5 "https://doi.org/10.25318/9810043401-eng"
  click s6 "https://theicct.org/publication/lvs-ci-quebec-can-en-feb22/"
  click s7 "https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada#a34"
  click s8 "https://atb.nlr.gov/transportation/2024/data"
  click s9 "https://www.epa.gov/regulations-emissions-vehicles-and-engines/optimization-model-reducing-emissions-greenhouse-gases"
  click p2 "https://github.com/RAMP-project/RAMP-mobility"
```

**Representative period clustering (pending refactor).** The current calculation
uses the ordinary mean of 8,760 equal-duration hours. After selecting or clustering
representative periods, rescale the charging shape using the final model time-slice
weights: cluster frequency × slice duration, normalized to fractions of the year.
An unweighted mean of representative slices can change annual utilization. #to-review

For representative shape $\boldsymbol{p_i}$, normalized annual weights $\boldsymbol{w_i}$, and annual charger
utilization target $\boldsymbol{U}$, use the same weights as the model's annual energy balance:
#to-review

```math
\mu_w = \sum_i w_i p_i,\quad \sum_i w_i = 1,\qquad
\mathrm{CF}_i = p_i\,\frac{U}{\mu_w},\quad
\sum_i w_i\,\mathrm{CF}_i = U.
```

For **`U = 0.20`**, the weighted mean of charging ceilings must remain 20%. Do not
multiply each **`CF_i`** by its weight again. The annual utilization floor remains
separate (`0.98 × U = 0.196` for new LDV chargers). Check the weighted mean and
`0 ≤ CF_i ≤ 1`; reject infeasible scaling rather than clipping. Apply this rescaling
in the upstream clustering step; the current transport projection requires a
complete mapping that preserves the full-year integral. #to-review

### Compact manual parameter selectors

Manual lifetime, efficiency, and cost tables use `technology_class` as an exact
selector for `technology.csv` `category`. When present, `powertrain` selects
`sub_category`; configured aliases reconcile reviewed label variants, `all`
selects the complete category, `remainder` excludes explicit peer selectors for
the same parameter, and a terminal year such as `_2035` is retained as
`selector_year`. `parameterization.manual_parameters` never edits the manual
tables: it publishes the row-to-technology expansion, reconciliation, registry,
and unmatched-selector findings under `inputs/1_interim/`.
