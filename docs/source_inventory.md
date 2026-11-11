# Transportation source inventory

!!! note "Source identity and quality have one owner"
    The [source registry](../config/sources.yaml) owns full citations, registered
    editions, access, validation and data-quality scores. This reference links the
    source families used by current builds and diagnostics; source quality does
    not independently validate the modeling assumptions used to transfer them.

## Parameter and mapping inputs

| Source | Primary publication or access | Current transport use | Native evidence |
| --- | --- | --- | --- |
| [*ANL BEAN*](https://publications.anl.gov/anlpubs/2023/11/179337.pdf) | [*Argonne Autonomie / BEAN 2022*](https://anl.app.box.com/s/an4nx0v2xpudxtpsnkhd5peimzu4j1hk/folder/242640145714) | Medium/heavy-truck M&R coefficients and bus maintenance proxies; [*Islam et al. (2022)*](https://publications.anl.gov/anlpubs/2023/11/179337.pdf) evidence | Selected MD/HD XLSM workbook |
| [*ANL HDSAM*](https://hdsam.es.anl.gov/index.php) | [*Hydrogen Delivery Scenario Analysis Model v4.5*](https://hdsam.es.anl.gov/index.php) | Hydrogen-refueling station lifetime | Manual CSV excerpt |
| *Canada EPS* | [*Canada Energy Policy Simulator v3.4.7*](https://docs.energypolicy.solutions/models/canada) | Motorcycle lifetime | Manual CSV excerpt |
| [*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) | [*Canada's Energy Future*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) | GDP-based demand growth and monetary conversions; end-use benchmarks and provisional fuel-price evidence | CSV |
| [*Dunsky*](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada) | [*Electric Vehicle Charging Infrastructure for Canada*](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada) | Charger utilization, port ratings/type mix and investment costs | Manual CSV excerpt |
| [*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) | [*National Energy Modeling System transportation inputs*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) | Selectable heavy-truck survival and lifetime proxy | XLSX |
| [*EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) | [*Population and Activity of Onroad Vehicles in EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) | Default car/light-truck/heavy survival and lifetimes; heavy schedule also proxies medium trucks. | PDF |
| [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) | [*EPA OMEGA v2.5 baseline*](https://github.com/USEPA/EPA_OMEGA_Model) | LDV BEV/PHEV sales-weighted range mix for existing archetypes, optional new-capacity shares and representative ranges | Native parent `aggregated_vehicles.csv` |
| [*EPRI REGEN*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) | [*US-REGEN v2025 transportation assumptions*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) | Intercity-bus prices/consumption; aviation efficiency improvement; rail/marine investment and efficiency ratios | HTML/JavaScript payload; manual CSV excerpts |
| [*FAA Economic Values*](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) | [*Economic Values 2024*](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) | Aircraft capacity, load, speed and utilization for service-cost normalization; passenger/freight maintenance costs | Two PDF sections |
| [*JGCRI GCAM*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) | [*JGCRI GCAM transport inputs*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) | Canadian motorcycle consumption, purchase costs and M&R | CSV |
| [*ANL GREET*](https://greet.anl.gov/greet/versions.html) / [*ANL GREET2*](https://greet.anl.gov/greet/versions.html) | [*Argonne R&D GREET 2025 Rev.1*](https://greet.anl.gov/greet/versions.html) | Road-vehicle embodied emissions, marine HFO/MDO intensity ratio and fuel-HHV reference | Registered XLSM workbook pair; manual CSV excerpt |
| [*ICCT EV CHARGE*](https://theicct.github.io/EVCHARGE-doc/versions/v1.2/) | [*EV CHARGE v1.2*](https://theicct.github.io/EVCHARGE-doc/versions/v1.2/) | LDV charging-loss evidence for the vehicle/infrastructure accounting boundary | Manual CSV excerpt |
| [*ICCT HDV CHARGE*](https://theicct.github.io/HDVCHARGE-doc/versions/v1.2/) | [*HDV CHARGE v1.2*](https://theicct.github.io/HDVCHARGE-doc/versions/v1.2/) | MHDV charging-loss evidence for the vehicle/infrastructure accounting boundary | Manual CSV excerpt |
| [*IEA The Future of Hydrogen*](https://www.iea.org/reports/the-future-of-hydrogen) | [*The Future of Hydrogen*](https://www.iea.org/reports/the-future-of-hydrogen) | Hydrogen-refueling efficiency | Manual CSV excerpt |
| [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) | [*Ontario vehicle population, Reports A/4/5*](https://data.ontario.ca/dataset/vehicle-population-data) | LDV class/age weights; Ontario medium-truck GVWR mix; truck, motorcycle and bus ages; retention diagnostics | Annual ZIPs containing text reports |
| [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) | [*2024 Final Rule Central Analysis*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) | Car, light-truck and medium-truck survival schedules and fixed median lifetimes | ZIP containing workbook inputs |
| [*NHTSA vPIC*](https://vpic.nhtsa.dot.gov/api/) | [*Vehicle Product Information Catalog*](https://vpic.nhtsa.dot.gov/api/) | Optional vehicle-type and make/model-year corroboration for reviewed class mappings | Request-scoped API JSON |
| *NLR Alternative Fueling Infrastructure* | [*Assessment of Alternative Fueling Infrastructure in the United States*](https://doi.org/10.2172/2447838) | Residential Level 1 charger installation cost | Manual CSV excerpt |
| [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) | [*Transportation Annual Technology Baseline 2024*](https://atb.nlr.gov/transportation/2024/data) | Road-vehicle price/consumption trajectories, PHEV utility-weighted energy shares, LDV M&R inputs and age-based mileage; includes [*Burnham et al. (2021)*](https://publications.anl.gov/anlpubs/2021/05/167399.pdf) evidence | ZIP containing CSV/XLSX |
| [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | [*Comprehensive Energy Use Database*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | Historical road stock, activity, fuel use and utilization; non-road activity/capacity from energy and intensity; baseline service demand and off-road equipment energy demand | XLS |
| [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) | [*Fuel Consumption Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) | Historical LDV fuel/electricity consumption and PEV ranges; class-mapping corroboration | Pinned CSV snapshots |
| [*OEO*](https://github.com/TemoaProject/oeo/tree/master/database_documentation) | [*Open Energy Outlook*](https://github.com/TemoaProject/oeo/tree/master/database_documentation) | Rail/marine operating-cost ratios; charger lifetime and fixed-cost ratio; separate H2 station investment/fixed costs | Manual CSV excerpts |
| *Pollution Probe/Mobility Futures Lab* | [*2024 Canadian EV Charging Experience Survey*](https://www.pollutionprobe.org/pollution-probe-2024-canadian-electric-vehicle-owner-charging-experience-survey-report/) | Private LDV Level 1/Level 2 charger mix | Manual CSV excerpt |
| [*RAMP-mobility*](https://github.com/RAMP-project/RAMP-mobility) | [*Registered Ontario charging simulations*](../config/sources.yaml) | Frozen [*US NHTS*](https://nhts.ornl.gov/)/[*Ontario TTS*](https://dmg.utoronto.ca/transportation-tomorrow-survey/)-derived LDV charging shapes, scaled to reviewed charger utilization | Minute-resolution model-output CSVs |
| [*SFU CIMS*](https://github.com/EMRG-SFU/cims/tree/main) | [*Canadian Integrated Modelling System*](https://github.com/EMRG-SFU/cims/tree/main) | Air/rail/marine capital costs and fixed lifetimes; rail/marine service-output assumptions | Manual CSV excerpts |
| [*StatCan*](https://www.statcan.gc.ca/en/developers/wds) | [*Statistics Canada transportation tables*](https://www.statcan.gc.ca/en/developers/wds) | Historical LDV/truck registration fuel shares, heavy-truck haul weights and provincial bus lifetimes | ZIPs containing CSV; API metadata JSON |
| [*Transport Canada iZEV*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) | [*Incentives for Zero-Emission Vehicles request records*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) | Purchase/lease acquisition records proxy PEV make/model composition; eligibility, request timing and unmatched identities limit representativeness | Pinned CSV snapshots |
| [*Transport Canada EV Dashboard*](https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard) | [*Canada Electric Vehicle Dashboard*](https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard) | Public LDV port counts/type mix and selectable medium-truck BEV stock-share proxy | HTML snapshot |
| [*US FuelEconomy.gov*](https://www.fueleconomy.gov/feg/ws/index.shtml) | [*Vehicle data*](https://www.fueleconomy.gov/feg/ws/index.shtml) | LDV class/HEV corroboration; separately registered PEV range evidence for review, without production sales weights | Shared ZIP containing `vehicles.csv` |
| [*Wards Intelligence*](https://wardsintelligence.informa.com/automotive-data) | [*Canadian vehicle sales by GVWR*](https://wardsintelligence.informa.com/automotive-data) | Medium-truck GVWR mix outside Ontario; optional LDV class-share legacy comparison | Reviewed aggregate CSV |

## Growth research inputs

!!! info "Research evidence for an upcoming feature"
    These sources support the adoption/growth diagnostic. They do not supply
    production growth constraints.

| Source | Primary publication or access | Current transport use | Native evidence |
| --- | --- | --- | --- |
| *EAFO Norway* | [*European Alternative Fuels Observatory — Norway*](https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/norway/vehicles-and-fleet) | Norwegian vehicle-stock and registration histories for growth comparisons | HTML plus 12 CSVs |
| *Extended HATCH* | [*National adoption and growth evidence*](https://zenodo.org/records/19579793) | Cross-technology national adoption histories and growth characteristics | CSV/XLSX |
| *HATCH* | [*Original HATCH observations and metadata*](https://zenodo.org/records/10231865) | Historical adoption observations and published growth estimates | CSV/YAML, including [growth estimates](https://zenodo.org/records/10056128) |

## Registered input locations

Downloads and API responses live in `inputs/0_cache/`;
registered workbooks in [`inputs/0_external_models/`](../inputs/0_external_models/);
and reviewed excerpts in [`inputs/0_manual_params/`](../inputs/0_manual_params/).
Registered [*RAMP-mobility*](https://github.com/RAMP-project/RAMP-mobility) outputs and the [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) parent retain their configured
locations under [`legacy_backend/`](../legacy_backend/). Backend-owned templates
are structural inputs with internal provenance rather than external-source citations.
