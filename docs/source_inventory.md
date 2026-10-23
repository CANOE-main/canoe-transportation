# Transportation source inventory

Abbreviations match the source names in [assumptions](assumptions.md). The
[source registry](../config/sources.yaml) owns full citations, editions, access,
validation and data quality. This inventory groups registered components by
source family; **Components** counts registry components, not physical files.

**Raw size** measures native inputs on **2026-10-23**, before backend
normalization. ZIPs count once in compressed form; extracted copies, generated
tables, result banks and databases are excluded. Reviewed manual components count their
cited CSV records, with each shared file's header counted once. These are the
backend's reviewed excerpts, not the size of the publisher's full model or report.
KiB, MiB and GiB use powers of 1,024. Totals use unrounded bytes.

## Parameter and mapping inputs

| Abbreviation | Source family | Main transport use | Components | Native inputs | Raw size |
| --- | --- | --- | ---: | --- | ---: |
| *ANL BEAN* | [Argonne Autonomie / BEAN 2022](https://anl.app.box.com/s/an4nx0v2xpudxtpsnkhd5peimzu4j1hk/folder/242640145714) | Medium/heavy-truck M&R coefficients and bus maintenance proxies; *Islam et al. (2022)* evidence | 1 | Selected MD/HD XLSM workbook | 16.07 MiB |
| *ANL HDSAM* | [Hydrogen Delivery Scenario Analysis Model v4.5](https://hdsam.es.anl.gov/index.php) | Hydrogen-refueling station lifetime | 1 | Manual CSV excerpt | 383 B |
| *Canada EPS* | [Canada Energy Policy Simulator v3.4.7](https://docs.energypolicy.solutions/models/canada) | Motorcycle lifetime | 1 | Manual CSV excerpt | 421 B |
| *CER CEF* | [Canada's Energy Future](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) | GDP-based demand growth and monetary conversions; end-use benchmarks and provisional fuel-price evidence | 3 | CSV | 9.79 MiB |
| *Dunsky* | [Electric Vehicle Charging Infrastructure for Canada](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada) | Charger utilization, port ratings/type mix and investment costs | 1 | Manual CSV excerpt | 18.45 KiB |
| *EIA NEMS* | [National Energy Modeling System transportation inputs](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) | Selectable heavy-truck survival and lifetime proxy | 1 | XLSX | 720.03 KiB |
| *EPA MOVES5* | [Population and Activity report, Table C-1](https://nepis.epa.gov/Exe/ZyPDF.cgi?Dockey=P101CUN7.pdf) | Default heavy-truck survival and lifetime proxy | 1 | PDF | 3.67 MiB |
| *EPA OMEGA* | [OMEGA v2.5 baseline](https://github.com/USEPA/EPA_OMEGA_Model) | LDV BEV/PHEV sales-weighted range mix for existing archetypes, optional new-capacity shares and representative ranges | 1 | Native parent `aggregated_vehicles.csv` | 498.10 KiB |
| *EPRI REGEN* | [US-REGEN v2025 transportation assumptions](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) | Intercity-bus prices/consumption; aviation efficiency improvement; rail/marine investment and efficiency ratios | 3 | HTML/JavaScript payload; manual CSV excerpts | 95.71 KiB |
| *FAA* | [Economic Values 2024](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) | Aircraft capacity, load, speed and utilization for service-cost normalization; passenger/freight maintenance costs | 2 | Two PDF sections | 1.45 MiB |
| *GCAM* / *GCAM Canada* | [JGCRI GCAM transport inputs](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) | Canadian motorcycle consumption, purchase costs and M&R | 1 | CSV | 1.00 MiB |
| *GREET* / *GREET2* | [Argonne R&D GREET 2025 Rev.1](https://greet.anl.gov/greet/versions.html) | Road-vehicle embodied emissions, marine HFO/MDO intensity ratio and fuel-HHV reference | 3 | Registered GREET1/GREET2 XLSM pair; manual CSV excerpt | 33.20 MiB |
| *ICCT EV CHARGE* | [EV CHARGE v1.2](https://theicct.github.io/EVCHARGE-doc/versions/v1.2/) | LDV charger efficiency | 1 | Manual CSV excerpt | 260 B |
| *ICCT HDV CHARGE* | [HDV CHARGE v1.2](https://theicct.github.io/HDVCHARGE-doc/versions/v1.2/) | MHDV charger efficiency | 1 | Manual CSV excerpt | 252 B |
| *IEA The Future of Hydrogen* | [The Future of Hydrogen](https://www.iea.org/reports/the-future-of-hydrogen) | Hydrogen-refueling efficiency | 1 | Manual CSV excerpt | 183 B |
| *MTO* | [Ontario vehicle population, Reports A/4/5](https://data.ontario.ca/dataset/vehicle-population-data) | LDV class/age weights; Ontario medium-truck GVWR mix; truck, motorcycle and bus ages; retention diagnostics | 3 | Annual ZIPs containing text reports | 22.88 MiB |
| *NHTSA CAFE* | [2024 Final Rule Central Analysis](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) | Car, light-truck and medium-truck survival schedules and fixed median lifetimes | 1 | ZIP containing workbook inputs | 8.79 MiB |
| *NHTSA vPIC* | [Vehicle Product Information Catalog](https://vpic.nhtsa.dot.gov/api/) | Optional vehicle-type and make/model-year corroboration for reviewed class mappings | 2 | Request-scoped API JSON | 9.88 MiB |
| *NLR Alternative Fueling Infrastructure* | [Assessment of Alternative Fueling Infrastructure in the United States](https://doi.org/10.2172/2447838) | Residential Level 1 charger installation cost | 1 | Manual CSV excerpt | 430 B |
| *NLR ATB* | [Transportation Annual Technology Baseline 2024](https://atb.nlr.gov/transportation/2024/data) | Road-vehicle price/consumption trajectories, PHEV utility-weighted energy shares, LDV M&R inputs and age-based mileage; includes *Burnham et al. (2021)* evidence | 7 | ZIP containing CSV/XLSX | 259.16 MiB |
| *NRCan CEUD* | [Comprehensive Energy Use Database](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | Historical road stock, activity, fuel use and utilization; non-road activity/capacity from energy and intensity; baseline service demand and off-road equipment energy demand | 22 (17 provincial + 5 national) | XLS | 6.15 MiB |
| *NRCan Ratings* | [Fuel Consumption Ratings](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) | Historical LDV fuel/electricity consumption and PEV ranges; class-mapping corroboration | 6 | Pinned CSV snapshots | 2.43 MiB |
| *OEO* | [Open Energy Outlook](https://github.com/TemoaProject/oeo/tree/master/database_documentation) | Rail/marine operating-cost ratios; charger lifetime and fixed-cost ratio; separate H2 station investment/fixed costs | 4 | Manual CSV excerpts | 5.90 KiB |
| *Pollution Probe/Mobility Futures Lab* | [2024 Canadian EV Charging Experience Survey](https://www.pollutionprobe.org/pollution-probe-2024-canadian-electric-vehicle-owner-charging-experience-survey-report/) | Private LDV Level 1/Level 2 charger mix | 1 | Manual CSV excerpt | 754 B |
| *RAMP-mobility* | [Registered Ontario charging simulations](../config/sources.yaml) | Frozen NHTS/TTS-derived LDV charging shapes, scaled to reviewed charger utilization | 2 | Minute-resolution model-output CSVs | 45.40 MiB |
| *SFU CIMS* | [Canadian Integrated Modelling System](https://github.com/EMRG-SFU/cims/tree/main) | Air/rail/marine capital costs and fixed lifetimes; rail/marine service-output assumptions | 2 | Manual CSV excerpts | 5.37 KiB |
| *StatCan* | [Statistics Canada transportation tables](https://www.statcan.gc.ca/en/developers/wds) | Historical LDV/truck registration fuel shares, heavy-truck haul weights and provincial bus lifetimes | 5 | ZIPs containing CSV; API metadata JSON | 78.74 MiB |
| *Transport Canada* | [Canada Electric Vehicle Dashboard](https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard) | Public LDV port counts/type mix and selectable medium-truck BEV stock-share proxy | 2 | HTML snapshot | 89.44 KiB |
| *US FuelEconomy.gov* | [Vehicle data](https://www.fueleconomy.gov/feg/ws/index.shtml) | LDV class/HEV corroboration; separately registered PEV range evidence for review, without production sales weights | 2 | Shared ZIP containing `vehicles.csv` | 2.08 MiB |
| *Wards* | [Canadian vehicle sales by GVWR](https://wardsintelligence.informa.com/automotive-data) | Medium-truck GVWR mix outside Ontario; optional LDV class-share legacy comparison | 1 | Reviewed aggregate CSV | 12.34 KiB |
| **Subtotal** | **31 registry sources, with provincial/national CEUD combined above** | | **83** | | **502.09 MiB** |

## Growth research inputs

These registered inputs support opt-in adoption/growth diagnostics. They do not
supply production growth constraints.

| Abbreviation | Source family | Main transport use | Components | Native inputs | Raw size |
| --- | --- | --- | ---: | --- | ---: |
| *EAFO Norway* | [European Alternative Fuels Observatory — Norway](https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/norway/vehicles-and-fleet) | Norwegian vehicle-stock and registration histories for growth comparisons | 1 | HTML plus 12 CSVs | 111.00 KiB |
| *Extended HATCH* | [National adoption and growth evidence](https://zenodo.org/records/19579793) | Cross-technology national adoption histories and growth characteristics | 1 | CSV/XLSX | 6.89 MiB |
| *HATCH* | [Original HATCH observations and metadata](https://zenodo.org/records/10231865) | Historical adoption observations and published growth estimates | 1 | CSV/YAML, including [growth estimates](https://zenodo.org/records/10056128) | 4.66 MiB |
| **Subtotal** | **3 registry sources** | | **3** | | **11.66 MiB** |
| **Total** | **All 34 registry sources** | | **86** | | **513.76 MiB (538,715,190 bytes)** |

## Size scope and recreated checkout

Downloads and API responses live in [`inputs/0_cache/`](../inputs/0_cache/),
registered workbooks in [`inputs/0_external_models/`](../inputs/0_external_models/),
and manual excerpts in [`inputs/0_manual_params/`](../inputs/0_manual_params/).
The registered RAMP-mobility outputs and OMEGA parent remain under
[`legacy_backend/`](../legacy_backend/). Backend-owned templates are structural
inputs, outside the external-source total.

The snapshot covers CEUD 2023 across configured regions, CER CEF 2026, all 11
registered MTO annual archives, the six ratings snapshots and the present vPIC
response cache. BEAN counts the consumed MD/HD workbook; GREET counts the two
registered workbooks. Other files shipped alongside those workbooks are excluded.
OMEGA counts its native parent, excluding the backend extract. Research sizes
include all pinned assets.

Allow **about 2 GiB for a recreated working checkout**, excluding `.git`, the Python
environment, dependency caches, playground files, test residue, scratch files and
notebook session snapshots. The estimate combines versioned files, native inputs
and current interim, processed, validation, SQLite and log artifacts, deduplicated by
path: **about 1.76 GiB** measured here. Build layers account for about **0.97 GiB**;
they are separate from the raw-source total. Coverage includes historical and
optional diagnostics plus several database/report variants, so a single scenario
can use less space; retained runs and broader source coverage increase it.
