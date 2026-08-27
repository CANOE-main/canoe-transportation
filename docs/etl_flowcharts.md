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

## Table of Contents

- [Flowchart legends](#flowchart-legends)
- [`existing_capacity`](#existing_capacity)
- [`demand`](#demand)
- [`limit_annual_capacity_factor`](#limit_annual_capacity_factor)
- [`lifetime_tech` and `lifetime_survival_curve`](#lifetime_tech-and-lifetime_survival_curve)
- [`efficiency`](#efficiency)
- [`cost_invest`](#cost_invest)
- [`cost_variable`](#cost_variable)

## Flowchart Legends

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
  p0@{shape: hex, label: "**Conditional switch**<br>*feature:* true or false"}
  p0_2@{shape: hex, label: "**Scenario selector**<br>*scenario:* current measures, net-zero, etc."}
  subgraph process["`**Group of sources or processes**`"]
  direction TB
    p1["`**Harmonization protocol**<br>• Briefly describes parameter-handling rules, declared in *config/parameters/rules.yaml*<br><br>• There can be several processes and/or rules, usually described in a table below the chart`"]
    p2("`**Marimo diagnostic notebook**<br>• Visualizes and compares inputs and evidence, and tests assumptions during development to diagnose ETL decisions and derive insights from sources`")
    p1 -- notebook.py --> p2
  end

  s0 -- required process ---> process
  s1 -. conditional process .-> p0 -. true .-> process
  s2 -- required process --> p0_2 --> process

  process --> o1[/"`**Parameter-ready output**<br>Parameter values inserted into SQLite databases<br>[describes units]`"/]
```

## `existing_capacity`

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

  subgraph expansion["`**Vehicle population evidence**`"]
  direction LR
    s1[("`**ON Ministry of Transport (MTO)**<br>• Report A: vehicle counts by make-model codes, model year, and status<br>• Report 5: counts by aggregated class (COMM, PASS, BUS), model year, and status`")]
    s2[("`**Quebec SAAQ #to-do**<br>Active vehicle counts by make-model, model year, and jurisdiction`")]
    s3[("`**Insurance Corp. of BC #to-do**<br>Vehicle counts by make-model, vintage, and size class; aggregated and indexed by two attributes at a time`")]
  end

  subgraph classes["`**Vehicle class mapping evidence**`"]
    s1_2[("`**NRCan Fuel Consum. Ratings**<br>Official vehicle make and model mappings into size classes`")]
    s1_3[("`**EPA Fuel Economy API**<br>Supporting vehicle make and model mappings into size classes`")]
  end

  s4[("`**StatCan table**<br>New LDV registrations by fuel type and province`")]
  s5[("`**StatCan table**<br>MHDV registrations by fuel type and province`")]

  %% --- Processes ---
  subgraph age["`**Age cohort derivation and diagnosis**`"]
    direction TB
    p1["`**Fleet age distribution**
    • *Road:* distribute baseline stock by age.
    *MTO Report A is mapped into NRCan CEUD cars and light trucks via MTO code-to-model inference; and Report 5 distributes bus and motorcycle age cohorts*<br>
    • *Off-road:* treat provincial energy use ÷ intensity as stock, then distribute by age`"]

    p1_2("`**Vehicle population mapping diagnosis**`"
    MTO make-model codes are difficult to map. This notebook visualizes:<br>
    • mapped fit-active stock
    • stock that cannot be mapped reliably 
    • vehicle class and vintage weights
    • Report A vs Report 5 age cohorts
    • MTO stock vs NRCan CEUD data 
    • survival rates from mapped cohorts)
    p1 -- vehicle_population_aggregation_mapping.py --> p1_2
  end
  
  s0 -- nrcan_ceud.py --> age
  s1 -- vehicle_population.py --> age
  s1_2 -- nrcan_ceud.py --> age
  s1_3 -- epa_ldv_classes.py --> age

  p2["`**Fleet powertrain distribution**<br>• *Road:* distribute age-specific stock by powertrain<br><br>• *Off-road:* incumbent techs mostly use diesel or jet fuel<br><br>• Aggregate into 5-year vintages`"]
  age -- road_aggregation.py --> p2
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
  click s1_2 "https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64"
  click s1_3 "https://www.fueleconomy.gov/feg/ws/index.shtml"
```

| Harmonization rule                                     | Affected classes      | Description                                                                                                                          |
| ------------------------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Fetch vehicle counts by inferred size class            | Cars and light trucks | Map make-model-vintage keys with the reviewed NRCan and FuelEconomy.gov evidence crosswalk                                       |
| Distribute stock by age                                | Road vehicles         | Distribute NRCan CEUD stocks over existing vintages using age cohort registrations                               |
| Treat energy use<sub>province</sub>÷intensity as stock | Off-road modes        | Air, rail, and marine fleet size are estimated from the available supply capacity to satisfy demand by vintage (in demand units)      |
| Distribute stock by age                                | Off-road modes        | Available demand supply by vintage is estimated with a fleet turnover approximation assuming an avg. annual retirement of 1÷lifetime |
| Distribute stock<sub>age</sub> by powertrain           | Cars and light trucks | Each stock by vintage gets distributed over vehicle market shares by fuel type                                                       |
| Distribute stock<sub>age</sub> by powertrain           | MD trucks             | Each stock by vintage gets distributed over vehicle registration shares by fuel type – mainly diesel and gasoline                    |

## `demand`

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
  s1[("`**CER Canada's Energy Future**<br>Real GDP projections by scenario<br>*Def. scenario:* current measures`")]

  %% --- Processes ---
  p0@{shape: hex, label: "**config/scenarios/**<br>*cer_scenario:* current, higher, lower, or net-zero"}
  p1["`**Baseline and projection**<br>• *Off-road:* estimate provincial activity as energy use ÷ intensity<br><br>• Index future demand to GDP growth by scenario`"]

  s0 -- nrcan_ceud.py --> p1
  s1 -- cer_enerfuture.py --> p0

  p0 --> p1
  p1 -- stocks_and_demands.py --> o1[/"`**demand**<br>[bn passenger-km]<br>[bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s0 "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"
  click s1 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
```

| Harmonization rule                                                      | Affected classes | Description                                                                                                    |
| ----------------------------------------------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------- |
| Estimate provincial activity as energy use<sub>province</sub>÷intensity | Off-road modes   | Provincial energy consumption [PJ] divided by national energy intensity [PJ/bn-tkm] as provincial demand proxy |
| Index future demand to GDP growth                                       | All              | Base year demand is indexed to future GDP growth projections by scenario                                       |

## `limit_annual_capacity_factor`

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
  s1[("`**NLR Annual Tech. Baseline**<br>Age-based annual mileage profiles (VMT schedules) of cars and LD/MD/HD trucks`")]
  s2@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}

  %% --- Processes ---
  p0["`**Annual vehicle utilization (UF)**<br>**eq. (i)** 5-year avg of activity ÷ stock excluding 2020-2021, then scaled by **capacity_to_activity**`"]
  s0 -- nrcan_ceud.py --> p0

  p1@{shape: hex, label: "**config/scenarios/**<br>*vkt_schedules:* true or false"}
  p0 -- stocks_and_demands.py --> p1

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

### Equations

```math
(i)\;\mathrm{UF[\text{-}]}=\frac{\mathrm{Activity[bn\;tonne\text{-}km/year]}}{\mathrm{Stock[k\;units]}\cdot \mathrm{C2A[bn\;t\text{-}km/k\;units\cdot year]}}
```

| Harmonization rule                          | Affected classes | Description                                                                                                                                                           |
| ------------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Annual utilization as activity÷stock ratios | Road vehicles    | Represents how much activity a unit of capacity can deliver annually. Utilization is scaled with an arbitrary `capacity_to_activity` factor to avoid near-zero values |
| Assume constant annual utilization          | Road vehicles    | Annual utilization derived from 5-year average ratios remains constant across all periods                                                                             |
| Aggregate profiles and normalize            | Road vehicles    | Aggregate annual mileage profiles by vehicle size/weight class using aggregation mappings (see `efficiency` flowchart) and apply max scaling to each series           |
| Scale utilization by normalized trajectory  | Road vehicles    | Baseline utilization is indexed through normalized age trajectories to obtain utilization as a function of vehicle age, mostly decaying                               |

## `lifetime_tech` and `lifetime_survival_curve`

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
  subgraph survival["`**Road vehicle fleet survival rates (US sources)**`"]
    direction LR
    s2[("`**NHTSA CAFE model**<br>LDV survival rates by vehicle size class`")]
    s3[("`**EIA NEMS model**<br>MD/HD truck survival rates by weight class`")]
  end

  subgraph population["`**Vehicle population evidence**`"]
  direction LR
    s0[("`**ON Ministry of Transport (MTO)**<br>• Report A: vehicle counts by make-model codes, model year, and status`")]
    s1[("`**Quebec SAAQ #to-do**<br>Active vehicle counts by make-model, model year, and jurisdiction`")]
  end
  
  s6@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}

  %% --- Processes ---
  subgraph age["`**Vehicle cohort mapping and survival rate estimation**`"]
    direction TB
    p0["`**Survival rate estimation**
    • **eq. (i)** Estimate apparent retirement from make-model-vintage cohorts across Report A editions before mapping<br>
    • The MTO make-model mapping separates LDV stock exposure from unmapped and non-LDV; and aggregates evidence accordingly<br>
    • **eq. (ii)** The empirical rates by class and age are the total apparent retirement (*D*) divided by total starting exposure (*E*)<br>
    • Because the resulting survival rates are very similar to aggregated NHTSA CAFE curves → **Only the latter are promoted as parameters**`"]

    p0_2("`**Vehicle population mapping diagnosis**`"
    MTO make-model codes are difficult to map. This notebook visualizes:<br>
    • mapped fit-active stock
    • stock that cannot be mapped reliably 
    • vehicle class and vintage weights
    • Report A vs Report 5 age cohorts
    • MTO stock vs NRCan CEUD data 
    • survival rates from mapped cohorts)
    p0 -- vehicle_population_aggregation_mapping.py --> p0_2
  end
  survival -- "assorted_sources.py" --> age
  s0 -. "`vehicle_population.py<br>*diagnostic-only*`" .-> age
  s6 -- road_aggregation.py --> age

  p1@{shape: hex, label: "**config/scenarios/**<br>*survival_curves:* true or false"}
  age -- lifetimes_survival.py --> p1

  s4[("`**StatCan table**<br>Buses avg. lifetime by province`")]
  s5@{shape: doc, label: "**SFU CIMS model assumptions**<br>Lifetime of remaining modes"}

  p2@{shape: hex, label: "**config/scenarios/**<br>*survival_curve_max_age: 25*<br>*Must be less than the time horizon"}
  p1 -. true .-> p2

  p3["`**Fixed lifetimes**<br>• Median lifetimes (p<sub>survival</sub>=0.5) by default when survival curves are disabled<br><br>• Get avg. lifetimes from remaining sources`"]
  p1 -- false --> p3
  s4 -- statcan_tables.py --> p3
  s5 -- inputs/0_manual_params/ --> p3

  p2 -. "lifetimes_survival.py" .-> o1[/"`**lifetime_survival_curve**<br>[-]`"/]
  p3 -- lifetimes_survival.py --> o2[/"`**lifetime_tech**<br>[years]`"/]

  %% --- Hyperlinks ---
  click s0 "https://data.ontario.ca/dataset/vehicle-population-data"
  click s1 "https://www.donneesquebec.ca/recherche/dataset/vehicules-en-circulation"
  click s2 "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/"
  click s3 "https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx"
  click s4 "https://doi.org/10.25318/3410025401-eng"
  click s5 "https://github.com/EMRG-SFU/cims-models/tree/main/sources/sectors"
```

### Equations

```math
(i)\quad r^{\mathrm{MTO}}_{p,k,v,a}=\frac{N_{p,k,v,a+1}}{N_{p,k,v,a}}; \qquad a=t-v
```
where $r$: apparent MTO-key survival rate, $p$: Report A class (PASS or COMM), $k$: abbreviated MTO make-model key, $v$: vintage of that key, $a$: vehicle age, $t$: Report A edition year, and $N_{p,k,v,a}$: observed FIT_ACTIVE stock.

```math
(ii)\quad R^{\mathrm{MTO}}_{c,a}=1 - \frac{\sum_v D_{c,v,a}}{\sum_v E_{c,v,a}}; \qquad E_{c,v,a}=\sum_{p,k → m(k,v)=c}N_{p,k,v,v+a}; \qquad D_{c,v,a}=\sum_{p,k → m(k,v)=c}\left(N_{p,k,v,v+a}-N_{p,k,v,v+a+1}\right)
```
where $R$: aggregated vehicle class survival rate, $c$: target vehicle class, $m(k,v)$ reviewed class assigned to a particular MTO-key and vintage, $E$: pooled starting exposure—the sum of beginning-of-transition stock contributing to a class-vintage-age estimate, $D$: aggregated apparent retirements.

```math
S_c(a+1)=S_c(a)R_{c,a}; \qquad S_c(0)=1
```
The cumulative MTO survival is a product of those empirical rates with an explicit age-zero baseline, following NHTSA indexing method. Because no vehicle-level identifier links editions, **apparent retirements mix physical retirement with administrative status changes, migration, imports, re-registration, and MTO-key changes**.

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Standardize source survival schedules | Road vehicles | Keep NHTSA cumulative survival values as reported. Convert NEMS annual scrappage rates into cumulative survival, starting at 100% at age zero. |
| Map source schedules to model classes | Road vehicles | Assign NHTSA schedules to car and light-truck classes, using the latest Wards shares to combine the light-truck schedules. Keep NEMS medium- and heavy-truck weight classes separate unless reviewed aggregation weights are available. |
| Compile fixed lifetimes | All | When survival curves are disabled, keep configured fixed lifetimes and fill missing road-vehicle values with the first age at which the accepted survival curve reaches 50% or less. Use StatCan or configured average lifetimes for the remaining modes. |
| Select the lifetime representation | All | Use `survival_curves` to select fixed lifetimes or accepted road-vehicle curves. `survival_curve_max_age` limits the curve and stock-age horizon; it is not an individual technology lifetime. |
| Estimate MTO annual changes before mapping | Cars and light trucks | Compare `FIT_ACTIVE` counts for the same make-model and model year in consecutive Report A editions. Keep increases as observed rather than forcing every annual ratio between zero and one. |
| Map and aggregate MTO diagnostics | Cars and light trucks | Attach reviewed vintage-range mappings only after the raw annual changes are calculated. Sum the starting and following counts by NLR and NRCan CEUD class and age before calculating class-level rates. |
| Limit and check MTO diagnostic evidence | Cars and light trucks | Use starting ages 0 through 35, including eligible pre-2000 model years. Report mapping coverage, age gaps, unusual rates, and sample support; do not fill missing ages or the older tail with NHTSA or NEMS values. |
| Keep MTO results diagnostic | Cars and light trucks | Treat the results as changes in registered stock, not direct observations of vehicle retirement. Compare them with accepted NHTSA curves only after the MTO series is derived; the current decision keeps NHTSA curves as CANOE parameters. |

**Notes:**

- **Execution boundary.** The default `parameterization.lifetimes_survival` command
  publishes accepted source-based curves and source-derived median lifetimes without
  loading MTO history or its reviewed mapping. `--mto-diagnostics` publishes the MTO
  review evidence; `--all` runs both paths.
- Detailed MTO filters, evidence checks, and comparisons are documented in
  `docs/insights/vehicle_population_aggregation_mapping.py`.

## `efficiency`

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
  subgraph road_agg["`**Road aggregation maps**`"]
  subgraph agg["`**Road vehicle population evidence**`"]
	  s3@{shape: doc, label: "**Wards Intelligence**<br>National LDV and MHDV sales from 2021 by make-model, and GVWR class; covers aggregation in provinces without detailed vehicle population data"}
	  s4[("`**ON Ministry of Transport (MTO)**<br>• Report A: vehicle counts by make-model codes, model year, and status<br>• Report 4: counts by gross weight bucket, (COMM, PASS, BUS), and status`")]
	  s10[("`**StatCan Tables**<br>Truck shipment distance and tonne-km where province is origin or destination`")]
    s5[("`**Quebec SAAQ #to-do**<br>Active vehicle counts by make-model, vintage, and inferred size class`")]
  end
  subgraph aggregation["`**Vehicle class aggregation and diagnosis**`"]
  direction TB
    p0["`**Road aggregation mapping**
    • ***LDVs:*** map vehicle size classes and derive efficiency aggregation weights
    *MTO Report A is mapped into NRCan and NLR classes via MTO make-model similarity*<br>
    • ***MD/HD** trucks:* map truck weight classes and derive efficiency aggregation weights
    *Report 4 distributes medium truck gross weight class cohorts*<br>
    • ***HD tr**ucks:* derive regional- and long-haul activity weights`"]
    p0_2("`**Vehicle population mapping diagnosis**`"
    MTO make-model codes are difficult to map. This notebook visualizes:<br>
    • mapped fit-active stock
    • what cannot be mapped reliably 
    • vehicle class and vintage weights 
    • Report A vs Wards LDV class shares 
    • Report 4 vs Wards MD truck shares 
    • survival rates from mapped cohorts)
    p0 -- vehicle_population_aggregation_mapping.py --> p0_2
  end
	s3 -- inputs/0_manual_params/ --> aggregation
  s4 -- vehicle_population.py --> aggregation
  s10 -- statcan_tables.py --> aggregation
  end

  subgraph road["`**Road efficiencies**`"]
	  s1[("`**NRCan Fuel Consum. Ratings**<br>Car and light-truck ratings by make and model`")]
	  s2[("`**Autonomie TEA via NLR ATB**<br>Future vehicle efficiencies and powertrain multipliers by scenario<br>*Def. scenario:* mid trajectory`")]
	  s2_2[("`**JGCRI GCAM model**<br>Motorcycle (>250 cc) efficiencies for Canada`")]
  end
  
  subgraph off["`**Off-road efficiencies**`"]
  direction LR
	  s7@{shape: docs, label: "**EPRI REGEN model assumptions**<br>Future multipliers for inter-city buses and off-road modes"}
	  s8@{shape: doc, label: "**EIA NEMS model assumptions**<br>Fuel consumption improvement of -1%/year for new jet aircrafts"}
  end
  
  s0[("`**NRCan CEUD**<br>Fleet energy intensity of medium/heavy trucks and off-road modes`")]
  s9[("`**NRCan CEUD**<br>Vehicle/mode occupancy and payload factors`")]

  %% --- Processes ---
  s0 -- nrcan_ceud.py --> p3 & p2

  p2_2@{shape: hex, label: "**config/scenarios/**<br>*atb_scenario:* mid, conservative, or advanced"}
  p2["`**Road baseline and indexing**<br>• *Existing LDVs:* aggregate fuel consumption ratings using mappings<br><br>• *Existing MD/HD trucks:* use incumbent fleet energy intensity<br><br>• *New road vehicles:* index existing efficiencies to aggregated future multipliers`"]
  s1 -- nrcan_ceud.py --> p2
  s2 -- nlr_atb_autonomie.py --> p2_2 --> p2
  s2_2 -- assorted_sources.py --> p2
  aggregation -- road_aggregation.py --> p2
  
  p3["`**Off-road baseline and indexing**<br>• *Existing off-road modes:* use incumbent fleet energy intensity<br><br>• *New off-road modes:* index existing efficiencies to future multipliers`"]
  off -- inputs/0_manual_params/ --> p3
    
  p4["`**Period & unit harmonization**<br>• Aggregate existing efficiencies into 5-year vintages<br><br>• Convert to service-output efficiency using load factors`"]
  s9 -- nrcan_ceud.py --> p4
  p2 -- efficiencies.py --> p4
  p3 -- efficiencies.py --> p4 -- efficiencies.py --> o1[/"`***efficiency***<br>[bn passenger-km/PJ]<br>[bn tonne-km/PJ]`"/]
  
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
| Map size classes and derive aggregation weights | Cars and light trucks | - Map MTO make-model-vintage keys from normalized NRCan Ratings, FuelEconomy.gov, and NHTSA vPIC API evidence<br>- Derive latest-snapshot fleet composition weights by mapped vehicle class |
| Map weight classes and derive aggregation weights | MD/HD trucks | Map truck weight-rating counts to classes that align with Autonomie truck projection classes |
| Derive regional- and long-haul activity weights | Heavy-duty trucks | Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes |
| Aggregate efficiency ratings using mappings | Cars and light trucks | Use size-class aggregation weights to convert model-level fuel consumption ratings into fleet-average efficiencies by powertrain |
| Use incumbent fleet energy intensity | MD/HD trucks and off-road | Use NRCan incumbent fleet energy intensities as proxies for existing technology efficiencies where fuel use is dominated by one fuel type |
| Index existing efficiencies to future multipliers | All | Apply alternative-powertrain and future-period multipliers to existing efficiencies (e.g., 2030 battery-electric and 2040 fuel-cell multipliers) |
| Special handling of buses | Transit, school, intercity | Use reported Autonomie values for existing and future transit and school bus efficiencies; use EPRI REGEN inputs for intercity buses |
| Special handling of motorcycles | Motorcycles | Use [PNNL GCAM](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) efficiencies |
| Convert to service-output efficiency units | All | Convert source efficiencies (e.g., L/100 km or mpg) into demand units (e.g., bn tonne-km/PJ) with NRCan CEUD load factors; using HHVs. |

### Vehicle make-model to vehicle classes mapping

`config/parameters/vehicle_size_class_map.csv` inherits all reviewed make-model mappings; see a few examples:

| **MTO Make-Model** | **Mapped Model** | **NRCan Fuel Ratings** | **Autonomie TEA (NLR ATB)** | **NRCan CEUD** |
| ------------------ | ---------------- | ---------------------- | ----------------- | -------------- |
| KIA OSL, KIA SOU   | **Kia Soul**     | Station Wagon: Small   | Midsize           | Car            |
| FORD F/E, FORD SRW | **Ford F-150**   | Pickup truck: Standard | Pickup            | Light Truck    |
| HON UDY, HON ODY   | **Toyota RV4**   | Minivan                | Midsize SUV       | Light Truck    |

## `cost_invest`

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
  s3[("`**CER Canada's Energy Future**<br>Currency exchange rates and GDP deflator by scenario<br>*Def. scenario:* current measures`")]
  subgraph offroad["`**Off-road CAPEX**`"]
	  %% --- Sources ---
	  s7@{shape: docs, label: "**SFU CIMS model assumptions**<br>Capital cost allocation of new off-road transportation in normalized units of demand"}
	  s8@{shape: docs, label: "**EPRI REGEN model assumptions**<br>CAPEX multipliers of alternative off-road modes"}
  end

  %% --- Processes ---
  p3["`**Cost of new off-road demand**<br>• Capital cost of building supply capacity to satisfy off-road demand *[dollars/demand unit]*<br><br>• Aircrafts' CAPEX normalized with utilization and load factors used in OPEX`"]
  s7 & s8 -- inputs/0_manual_params/ --> p3

  subgraph road["`**Road vehicle costs**`"]
	  %% --- Sources ---
	  s1@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
	  s2[("`**Autonomie TEA via NLR ATB**<br>Modeled vehicle price by class and powertrain by scenario<br>*Def. scenario:* mid trajectory`")]
    s8_bus@{shape: doc, label: "**EPRI REGEN model assumptions**<br>Vehicle price projections of intercity buses"}
  end

  %% --- Processes ---
  p0_2@{shape: hex, label: "**config/scenarios/**<br>*atb_scenario:* mid, conservative, advanced"}
  p2["`**Vehicle manufacturing costs**<br>• Revert vehicle prices back to manufacturing costs, divide by the RPE markup factor of 1.5<br><br>• Aggregate manufacturing cost projections by vehicle class using efficiency mappings`"]
  s2 -- nlr_atb_autonomie.py --> p0_2 --> p2
  s1 -- road_aggregation.py --> p2
  s8_bus -- assorted_sources.py --> p2

  p0@{shape: hex, label: "**config/scenarios/**<br>*cer_scenario:* current, higher, lower, or net-zero"}
  p4["`**Harmonize currency units**<br>• Apply exchange rate to CAD<br>(e.g., 2023USD → 2023CAD)<br><br>• Discount to reference year<br>(e.g., 2023CAD → 2020CAD)<br><br>• Harmonize magnitude of denominators`"]
  p2 -- capex_opex.py --> p4
  p3 -- capex_opex.py --> p4
  s3 -- cer_enerfuture.py --> p0 --> p4

  p4 -- capex_opex.py --> o1[/"`***cost_invest***<br>[$M 2020CAD/k vehicles]<br>[$M 2020CAD/bn passenger-km]<br>[$M 2020CAD/bn tonne-km]`"/]

  %% --- Hyperlinks ---
  click s8_bus "https://us-regen-docs.epri.com/v2025/assumptions/transportation.html#on-road-fleet-vehicles"
  click s8 "https://us-regen-docs.epri.com/v2025/assumptions/transportation.html#non-road-vehicles"
  click s7 "https://github.com/EMRG-SFU/cims-models/tree/main/sources/sectors"
  click s2 "https://vms.taps.anl.gov/research-highlights/vehicle-technologies/u-s-doe-vto-hfto-r-d-benefits/"
  click s3 "https://open.canada.ca/data/en/dataset/07c42deb-9435-43b9-a416-7ce316f3893d"
```

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps | - Cars and light trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with NRCan efficiency ratings and Autonomie projections<br>- Map truck weight-rating counts to classes that align with Autonomie truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes |
| Revert vehicle prices back into manufacturing costs | Road vehicles | A manufacturing to retail price equivalent markup factor (RPE) of 1.5 is generally used in Autonomie TEA and TCO study series |
| Aggregate manufacturing costs using mappings | Cars and trucks | Use mapped weights to aggregate Autonomie projected manufacturing costs of cars, LD, MD, and HD trucks, and school and transit buses |
| CAPEX of new off-road demand capacity | Off-road modes | Obtain input capital costs of building new supply capacity that satisfies off-road demand, derived as dollars per demand unit |
| Special handling of buses | Intercity buses | Use EPRI REGEN vehicle purchase cost assumptions for intercity buses |
| Special handling of motorcycles | Motorcycles | Use [PNNL GCAM](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) purchase costs |
| Harmonize currency units | All | Convert values using foreign currencies and/or different dollar years into reference currency-year |

## `cost_variable`

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
	  s8@{shape: docs, label: "**FAA Benefit-Cost Analysis**<br>• *Table 4-7 & 4-8:* Passenger & cargo aircraft avg. maintenance costs/block-hour<br>• *Table 3-6 & 3-9:* Average block speeds, aircraft capacities, and load factors"}
  end

  %% --- Processes ---
  p3["`**Variable costs from off-road**<br>• *Aircrafts:* **eq. (i-ii)** normalized maintenance costs per demand unit (CAPEX uses same factors) <br><br>• *Other off-road:* estimate variable costs with OEO ratios`"]
  s7 -- inputs/0_manual_params/ --> p3
  s8 -- assorted_sources.py --> p3

  subgraph road["`**Road M&R costs**`"]
	  %% --- Sources ---
	  s1@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
	  s2[("`**NLR ATB (Burnham et al. 2021)**<br>Avg. maintainance costs per mile, size and powertrain multipliers, and repair cost coefficients for LDVs<br><br>**NLR ATB (Autonomie TEA)**<br>Modeled vehicle price by class and powertrain by scenario<br>*Def. scenario:* mid trajectory`")]
	  s3@{shape: win-pane, label: "**ANL BEAN (Islam et al. 2022)**<br>Maintainance and repair linear model coefficients for MHDVs"}
  end

  %% --- Processes ---
  p0_2@{shape: hex, label: "**config/scenarios/**<br>*atb_scenario:* mid, conservative, or advanced"}
  p2["`**Maintainance & repair costs**<br>• *LDVs:* **eq. (i)** get age-dependent repair cost via empirical model;<br>**eq. (ii)** add avg. maintenance costs per mile (Burnham et al. 2021)<br><br>• *MHDVs:* **eq. (iii)** age-dependent M&R costs via empirical model (Islam et al. 2022)<br><br>• Aggregate M&R cost-per-mile curves by vehicle class using efficiency mappings`"]
  s2 -- nlr_atb_autonomie.py --> p0_2 --> p2
  s3 -- inputs/0_external_models/ --> p2
  s1 -- road_aggregation.py --> p2

  s0[("`**CER Canada's Energy Future**<br>Currency exchange rates and GDP deflator by scenario<br>*Def. scenario:* current measures`")]
  s9[("`**NRCan CEUD**<br>Vehicle/mode occupancy and payload factors`")]

  p0@{shape: hex, label: "**config/scenarios/**<br>*cer_scenario:* current, higher, lower, or net-zero"}
  p4["`**Harmonize currency units**<br>• Apply exchange rate to CAD<br>(e.g., 2023USD → 2023CAD)<br><br>• Discount to reference year<br>(e.g., 2023CAD → 2020CAD)<br><br>• Harmonize magnitude and units of denominators`"]
  p2 -- capex_opex.py --> p4
  p3 -- capex_opex.py --> p4
  s0 -- cer_enerfuture.py --> p0 --> p4
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

### Road M&R equations

```math
(i)\; \mathrm{Repair}^{LDV}_{age}=\mathrm{size}\cdot \mathrm{pwt}\cdot C_{age}\cdot e^{\beta\cdot \mathrm{price}}
```

```math
(ii)\; \mathrm{M\&R}^{LDV}_{age}=\mathrm{Repair}^{LDV}_{age}+\mathrm{Maint.}^{LDV}
```

```math
(iii)\; \mathrm{M\&R}^{MHDV}_{age}=\mathrm{pwt}(m\cdot \mathrm{age}+b)
```

### Off-road M&R equations

```math
(i)\; \mathrm{M\&R}^{Air}=\frac{\mathrm{Cost\;per\;block\text{-}hour}}{\mathrm{Block\;speed}\cdot \mathrm{Seats}\cdot \mathrm{Load\;factor}}
```

```math
(ii)\; \mathrm{M\&R}^{Air}=\frac{\mathrm{Cost\;per\;block\text{-}hour}}{\mathrm{Block\;speed}\cdot \mathrm{Tonnes}\cdot \mathrm{Load\;factor}}
```

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps | - Cars and light trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with NRCan efficiency ratings and Autonomie projections<br>- Map truck weight-rating counts to classes that align with Autonomie truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes |
| Vehicle prices used in repair cost empirical model | Road vehicles | Minimum suggested retail prices (MSRP) are simply manufacturing cost outputs from Autonomie scaled evenly by a markup factor of 1.5; these are used for repair cost estimates, as done in Burnham et al. 2021 |
| Aggregate M&R costs using mappings | Cars and trucks | Use mapped weights to aggregate M&R cost curves of cars, LD, MD, and HD trucks, and transit buses |
| Variable costs from off-road | Off-road modes | - Estimate annual variable costs of rail and marine vessels with CAPEX-to-OPEX ratios used in the OEO model<br>- Estimate M&R costs per demand unit with empirical data from the FAA, converting miles to km and tons to tonnes; same annual utilization and load factors are used for CAPEX and OPEX |
| Special handling of buses | School and intercity buses | Assume same CAPEX-to-OPEX ratios as those from transit buses |
| Special handling of motorcycles | Motorcycles | Use [PNNL GCAM](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) Canada transportation inputs from `UCD_trn_data_CORE.csv` for future motorcycle (engine >250 cc) M&R costs |
| Harmonize currency units | All | - Convert values using foreign currencies and/or different dollar years into reference currency-year<br>- Harmonize denominators; convert miles into km and multiply by NRCan CEUD load factors |

### Notes

FAA source PDFs: [Section 3 — Aircraft Capacity and Utilization Factors](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost/econ-value-section-3-capacity.pdf) and [Section 4 — Aircraft Operating Costs](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost/econ-value-section-4-op-costs.pdf).

## `emission_embodied`

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
  subgraph greet["`**Argonne National Lab GREET model**`"]
    s1@{shape: win-pane, label: "**GREET_1 (fuel-cycle) and GREET_2 (vehicle-cycle) Excel models**<br>Solved with default inputs for model year 2025, can vary through 2050; each copy is solved for the following pair of classes:<br><br>• Cars and Class 6 trucks<br>• SUVs and Class 8 Day cab trucks<br>• Pickup and Class 8 Sleeper cab trucks"}
    s3@{shape: docs, label: "**Solved GREET model copies**<br>Vehicle-cycle lifetime emissions by scenario<br>*Def. scenario*: conventional materials"}
    s1 -- "`*manually-executed, saved*`" --> s3
  end

  %% --- Processes ---
  p1@{shape: hex, label: "**config/scenarios/**<br>*embodied_emissions:* true or false<br>*embodied_materials*: conventional or lightweight"}
  s2@{shape: processes, label: "**Road aggregation maps**<br>Reuse aggregation weights for LDV size, MD/HD truck weight, and HD truck haul classes; see *efficiency* diagram"}
  greet -. inputs/0_external_models/ .-> p1

  p2["`**Vehicle manufacturing emissions**<br>• Normalize units as k tonnes/k vehicles, including CO<sub>2</sub>, CH<sub>4</sub>, and N<sub>2</sub>O factors <br><br>• Aggregate light, medium, and heavy truck classes using efficiency mappings`"]
  p1 -. true .-> p2
  s2 -- road_aggregation.py --> p2

  p2 -. "emissions.py" .-> o1[/"`**emission_embodied**<br>[k tonnes/k vehicles]`"/]

  %% --- Hyperlinks ---
  click s1 "https://greet.anl.gov/"
```

| Harmonization rule | Affected classes | Description |
| --- | --- | --- |
| Reuse road aggregation maps | - Cars and LD trucks<br>- MD/HD trucks<br>- Heavy-duty trucks | - Map vehicle make/model counts to size classes that align with NRCan efficiency ratings and Autonomie projections<br>- Map truck weight-rating counts to classes that align with Autonomie truck projection classes<br>- Group HD truck tonne-km into regional- and long-haul activity buckets to aggregate Autonomie haul classes |
| Vehicle manufacturing emissions | Cars and trucks | - Default vehicle-lifetime emissions from GREET-2 are extracted directly, solved for model year 2025 and normalized by k tonnes per thousand vehicles manufactured; model years 2030 through 2050 remain available.<br>- GREET 2 calculates the emissions associated with the production and processing of vehicle materials, the manufacturing and assembly of the vehicle, and the EOL decomissioning. EOL credits from material recycling are not considered. Emissions from the transportation of raw and processed materials for each process step are neglected. |

## `capacity_factor_tech` for BEV charging profiles; pending refactor

A light-duty BEV charging demand profile is an aggregated hourly time-series from charging events across 8,760 hours with 15-min resolution from a simulated representative fleet of 2,500 BEVs using RAMP-mobility. They're currently not fetched by canoe-transportation v2 and are taken directly from the `legacy_backend/` evidence directory. New charging profiles with updated assumptions are pending simulation. #to-do

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
  subgraph vehicle["`**Vehicle fleet characteristics; these inputs are actively fetched for other parameters**`"]
  direction TB
    s3[("`**StatCan table**<br>Registered vehicle size class shares by province; regardless of powertrain type`")]
    s8[("`**Autonomie TEA via NLR ATB**<br>Modeled battery capacity at beginning of life by BEV size class`")]
    s9@{shape: win-pane, label: "**EPA OMEGA LD Central Case outputs**<br>Representative battery capacities and CD ranges by BEV size class based on modeled market composition"}
  end

  subgraph legacy["`**legacy_backend/; currently not fetched by canoe-transportation v2**`"]
    s0[("`**Renewables Ninja weather profiles**<br>Population-weighted, annual temperature profiles by province from MERRA-2 global dataset.`")]
  direction LR
    subgraph trip_other["`**Other household travel surveys**`"]
      s1_2[("`**2016 Tomorrow Transportation Survey**<br>Same survey parameters as the US NHTS; limited to the Ontario GGH region, excluding weekends. *2022 survey data is available upon request*.`")]
      s2[("`**Canadian Survey on Everyday Travel**<br>Announced as part of the 2026 Census of Population; yet to be released. #to-do`")]
    end

    subgraph trip["`**Trip characteristics and behavior**`"]
      direction LR
      s1[("`**FHA National Household Travel Survey**<br>• Trips by mode, purpose, distance, duration, and type of day<br>• 24-hour trip start/end times by day type<br>• Trips by vehicle/fuel type and who drove`")]
      p1["`**Travel behavior from a typical week; no seasonal variability**<br>• *Curate trip data*: exclude outliers, unrelated datapoints, and unclear responses<br><br>• *Map trip purposes:* classify trip reasons into personal- and occupation-related<br><br>• *Avg. trip characteristics:* get avg. daily driven distance and trip distance and duration by weekday and purpose<br><br>• *Get trip start-time distributions:* group trips by weekday and occupation and get trips' relative frequency by hour<br><br>• *Get main trip-occurrence windows:* hours when workers, students, and inactive cohorts drive the most`"]
      s1 -- charging_profiles/ --> p1
    end

    subgraph driver["`**Driver occupation composition**`"]
      s4[("`**StatCan 2021 Census of Population**<br>Share of population in the labour force by province; unspecified driving status`")]
      s5[("`**StatCan 2021 Census of Population**<br>Share of population attending postsecondary school by province; unspecified driving status`")]
    end

    subgraph charger["`**Charger characteristics and availability**`"]
      s6@{shape: doc, label: "**ICCT 2022 Quebec Charging Infrastructure Assessment**<br>Shares of EV owners with access to home and workplace charging, respectively"}
      s7@{shape: doc, label: "**NRCan/Dunsky 2024 EV Charging Infrastructure Assessment**<br>Projected total LDV charging ports by type (L1, L2, and DCFC) through 2050<br>"}
    end
  end

  %% --- Processes ---
  subgraph ramp["`**legacy_backend/; simulation runs done externally**`"]
  direction TB
    p2@{shape: win-pane, label: "**RAMP-mobility simulation model**<br>Stochastic BEV fleet aggregation framework; default parameters are:<br><br>• Representative fleet size of 2,500 BEVs<br><br>• Stochastic variability of total daily distance (±30%), avg. trip speed (±30%), battery consumption (±10%)<br><br>•Probability of charging during parking as logistic p(SOC); p(<0.2)=1 and p(>0.8)=0<br><br>• Probability of mobility events during (p=1) and outside (p=1/7) main windows"}
    p3["`**Resample and index charging profiles**<br>• Rolling average resamples annual 15-min charging demand time series to 8,760 h<br><br>• Time zones index referenced to America/Toronto (EDT)`"]
  end
  p2 -- charging_profiles/ --> p3
  legacy -- charging_profiles/ --> ramp
  vehicle -- spreadsheet_database/ --> ramp

  ramp -- ev_chargers.py --> o1[/"`***capacity_factor_tech***<br>[-]`"/]

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

## EV charger parameters

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
  s1@{shape: doc, label: "**NRCan/Dunsky 2024 EV Charging Infrastructure Assessment**<br>• *Annual LD UFs:* Table 31; average LDV charging port utilization rate<br><br>• *EV-to-port ratios*: Tables 7 and 37; LD and MHD EVs for every charger port, including residential<br><br>• *Home charger assumptions:* 10% of LD BEVs and 25% of PHEVs use L1; the remainder use L2<br><br>• *MHD chargers:* Table 19 assumptions for power levels, daily throughput, and charger type shares<br><br>• *LDV and MHDV Charger CAPEX:* Tables 12 and 21; per-port installation and equipment cost estimates"}

  s2@{shape: doc, label: "**Transport Canada EV Dashboard**<br>• *Total public LD chargers by type:* Cumulative number of public L2 and DCFC chargers<br><br>• *Total public LD chargers by province:* Aggregated total public LD charger count"}  

  %% --- Processes --- #to-do - note that I started doing shat I shouldn't in 
  p1["`**Annual EV charger utilization rate**<br>Maximum annual utilization represent how much load chargers can provide anually;<br><br>• *LDVs*: 15% in 2025 to 20% through 2050<br><br>• *MHDVs*: Dunsky estimates of charging time, vehicles/day served, and charger type shares land around 25-35% in 2025`"]
  p1_2@{shape: hex, label: "**inputs/0_manual_params/**<br>*LDV/MHD_charger_UF* ∈ (0, 1.0) ∀ future_period; defaults are:<br>*• LDV_charger_UF* = [0.15, 0.2, 0.2, ...]<br>*• MHDV_charger_UF* = [0.25, 0.3, 0.35, ...]"}
  s1 --> p1 --> p1_2
  p1_2 -- ev_chargers.py --> o1[/"`***limit_annual_capacity_factor***<br>*Operator: ≤*<br>*Indexed by period`"/]

  p2["`**EV-to-charger port ratios**<br>Total EVs for every charger port; Dunsky assumes 1 for LDVs and 1.5 for MHDVs, nationally. Used to estimate *existing_capacity* of charging infrastructure*`"]
  p2_2@{shape: hex, label: "**config/scenarios/**<br>• *LDEV_to_charger_ratio = 1<br>*• *MHDEV_to_charger_ratio* = 1.5"}
  s1 --> p2 --> p2_2



  %% --- Hyperlinks ---
  click s1 "https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada#a34"
  click s2 "https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard"
```

## Market share and technology adoption constraints

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


  %% --- Processes ---


  %% --- Hyperlinks ---
```

## Compact manual parameter selectors

Manual lifetime, efficiency, and cost tables use `technology_class` as an exact
selector for `technology.csv` `category`. When present, `powertrain` selects
`sub_category`; configured aliases reconcile reviewed label variants, `all`
selects the complete category, `remainder` excludes explicit peer selectors for
the same parameter, and a terminal year such as `_2035` is retained as
`selector_year`. `parameterization.manual_parameters` never edits the manual
tables: it publishes the row-to-technology expansion, reconciliation, registry,
and unmatched-selector findings under `inputs/1_interim/`.
