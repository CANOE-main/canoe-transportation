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
| [*Dunsky*](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada) | [*Electric Vehicle Charging Infrastructure for Canada*](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada) | Extracted charger costs, ratings, evolving mix and operating assumptions for both representations. Reviewed manual values supply supplemental inputs and aggregate utilization overrides. | Pinned report HTML and registered table selectors, with reviewed CSV supplements |
| [*EIA NEMS*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) | [*National Energy Modeling System transportation inputs*](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) | Selectable heavy-truck survival and lifetime proxy | XLSX |
| [*EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) | [*Population and Activity of Onroad Vehicles in EPA MOVES*](https://www.epa.gov/system/files/documents/2026-09/420r26010.pdf) | Default car/light-truck/heavy survival and lifetimes; heavy schedule also proxies medium trucks. | PDF |
| [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) | [*EPA OMEGA v2.5 baseline*](https://github.com/USEPA/EPA_OMEGA_Model) | LDV BEV/PHEV sales-weighted range mix for existing archetypes, optional new-capacity shares and representative ranges | Native parent `aggregated_vehicles.csv` |
| [*EPRI REGEN*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) | [*US-REGEN v2025 transportation assumptions*](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) | Intercity-bus prices/consumption; aviation efficiency improvement; rail/marine investment and efficiency ratios | HTML/JavaScript payload; manual CSV excerpts |
| [*FAA Economic Values*](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) | [*Economic Values 2024*](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) | Aircraft capacity, load, speed and utilization for service-cost normalization; passenger/freight maintenance costs | Two PDF sections |
| [*JGCRI GCAM*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) | [*JGCRI GCAM transport inputs*](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) | Canadian motorcycle consumption, purchase costs and M&R | CSV |
| *Jonas et al.* | [*Electric Vehicle User Behavior: An Analysis of Charging Station Utilization in Canada*](https://doi.org/10.3390/en16041592) | 2018–2019 BC and Quebec residential L2 telemetry supports the 0.08 home utilization proxy in explicit mode. | Reviewed Table 2 values in the registry |
| [*ANL GREET*](https://greet.anl.gov/greet/versions.html) / [*ANL GREET2*](https://greet.anl.gov/greet/versions.html) | [*Argonne R&D GREET 2025 Rev.1*](https://greet.anl.gov/greet/versions.html) | Road-vehicle embodied emissions, marine HFO/MDO intensity ratio and fuel-HHV reference | Registered XLSM workbook pair; manual CSV excerpt |
| [*ICCT EV CHARGE*](https://theicct.github.io/EVCHARGE-doc/versions/v1.2/) | [*EV CHARGE v1.2*](https://theicct.github.io/EVCHARGE-doc/versions/v1.2/) | LDV charging-loss evidence for the vehicle/infrastructure accounting boundary | Manual CSV excerpt |
| [*ICCT HDV CHARGE*](https://theicct.github.io/HDVCHARGE-doc/versions/v1.2/) | [*HDV CHARGE v1.2*](https://theicct.github.io/HDVCHARGE-doc/versions/v1.2/) | MHDV charging-loss evidence for the vehicle/infrastructure accounting boundary | Manual CSV excerpt |
| [*IEA The Future of Hydrogen*](https://www.iea.org/reports/the-future-of-hydrogen) | [*The Future of Hydrogen*](https://www.iea.org/reports/the-future-of-hydrogen) | Hydrogen-refueling efficiency | Manual CSV excerpt |
| [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) | [*Ontario vehicle population, Reports A/4/5*](https://data.ontario.ca/dataset/vehicle-population-data) | LDV class/age weights; Ontario medium-truck GVWR mix used in all modeled regions by default; truck, motorcycle and bus ages; retention diagnostics | Annual ZIPs containing text reports |
| [*NHTSA CAFE*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) | [*2024 Final Rule Central Analysis*](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) | Car, light-truck and medium-truck survival schedules and fixed median lifetimes | ZIP containing workbook inputs |
| [*NHTSA vPIC*](https://vpic.nhtsa.dot.gov/api/) | [*Vehicle Product Information Catalog*](https://vpic.nhtsa.dot.gov/api/) | Optional vehicle-type and make/model-year corroboration for reviewed class mappings | Request-scoped API JSON |
| *NREL Alternative Fueling Infrastructure* | [*Assessment of Alternative Fueling Infrastructure in the United States*](https://doi.org/10.2172/2447838) | Residential Level 1 charger installation cost range | Reviewed midpoint in manual CSV |
| [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) | [*Transportation Annual Technology Baseline 2024*](https://atb.nlr.gov/transportation/2024/data) | Road-vehicle price/consumption trajectories, PHEV utility-weighted energy shares, LDV M&R inputs and age-based mileage; includes [*Burnham et al. (2021)*](https://publications.anl.gov/anlpubs/2021/05/167399.pdf) evidence | ZIP containing CSV/XLSX |
| [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | [*Comprehensive Energy Use Database*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | Historical road stock, activity, fuel use and utilization; non-road activity/capacity from energy and intensity; baseline service demand and off-road equipment energy demand | XLS |
| [*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) | [*Fuel Consumption Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) | Historical LDV fuel/electricity consumption and PEV ranges; class-mapping corroboration | Pinned CSV snapshots |
| [*OEO*](https://github.com/TemoaProject/oeo/tree/master/database_documentation) | [*Open Energy Outlook*](https://github.com/TemoaProject/oeo/tree/master/database_documentation) | Rail/marine operating-cost ratios; charger lifetime and fixed-cost ratio; separate H2 station investment/fixed costs | Manual CSV excerpts |
| *Pollution Probe/Mobility Futures Lab* | [*2024 Canadian EV Charging Experience Survey*](https://www.pollutionprobe.org/pollution-probe-2024-canadian-electric-vehicle-owner-charging-experience-survey-report/) | Private LDV Level 1/Level 2 charger mix | Manual CSV excerpt |
| [*RAMP-mobility*](https://github.com/RAMP-project/RAMP-mobility) | [*Registered Ontario charging simulations*](../config/sources.yaml) | Six cases combine 2022 US NHTS or 2016 Ontario TTS travel inputs with uncontrolled, night or day charging on the 2018 Toronto calendar. Production reads the selected charging shape for aggregated LDV chargers. The notebook also checks battery mobility and active drivers. | Minute-resolution model-output CSVs and simulation manifests |
| [*SFU CIMS*](https://github.com/EMRG-SFU/cims-models/tree/main/csv/model) | [*Canadian Integrated Modelling System*](https://github.com/EMRG-SFU/cims-models/tree/main/csv/model) | Air/rail/marine capital costs and fixed lifetimes; rail/marine service-output assumptions | Manual CSV excerpts |
| [*StatCan*](https://www.statcan.gc.ca/en/developers/wds) | [*Statistics Canada transportation tables*](https://www.statcan.gc.ca/en/developers/wds) | Historical LDV/truck registration fuel shares, heavy-truck haul weights and provincial bus lifetimes | ZIPs containing CSV; API metadata JSON |
| [*Transport Canada iZEV*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) | [*Incentives for Zero-Emission Vehicles request records*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) | Purchase/lease acquisition records proxy PEV make/model composition; eligibility, request timing and unmatched identities limit representativeness | Pinned CSV snapshots |
| [*AFDC Alternative Fueling Station Locator*](https://afdc.energy.gov/stations/) | Canadian public/open electric station inventory | Observed provincial L1/L2/DCFC port counts; December 31, 2025 snapshot | Pinned API JSON |
| *US BEA* | [*NIPA Table 1.1.9, April 2024 release*](https://downloads.regulations.gov/BOEM-2024-0037-0002/content.pdf) | Rebase the ATB retail price input from 2022 USD to 2020 USD for the LDV repair equation. | Reviewed annual index levels in the registry |
| [*US FuelEconomy.gov*](https://www.fueleconomy.gov/feg/ws/index.shtml) | [*Vehicle data*](https://www.fueleconomy.gov/feg/ws/index.shtml) | LDV class/HEV corroboration; separately registered PEV range evidence for review, without production sales weights | Shared ZIP containing `vehicles.csv` |
| [*Wards Intelligence*](https://wardsintelligence.informa.com/automotive-data) | [*Canadian vehicle sales by GVWR*](https://wardsintelligence.informa.com/automotive-data) | Optional medium-truck GVWR mix and LDV class-share legacy comparison | Reviewed aggregate CSV |

## Additional diagnostic inputs {#growth-research-inputs}

The travel-survey and growth notebooks prepare these sources explicitly.
Production charging consumes the registered RAMP outputs above. Growth
constraints remain a research experiment.

| Source | Primary publication or access | Current transport use | Native evidence |
| --- | --- | --- | --- |
| *US NHTS* | [*2022 NextGen National Household Travel Survey, v2.1*](https://nhts.ornl.gov/) | Travel screening, timing and driver/trip summaries for the travel-survey notebook and RAMP input derivation. | Pinned public-use ZIP, codebook and technical documents |
| *EAFO Norway* | [*European Alternative Fuels Observatory — Norway*](https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/norway/vehicles-and-fleet) | Norwegian vehicle-stock and registration histories for growth comparisons | HTML plus 12 CSVs |
| *Extended HATCH* | [*National adoption and growth evidence*](https://zenodo.org/records/19579793) | Cross-technology national adoption histories and growth characteristics | CSV/XLSX |
| *HATCH* | [*Original HATCH observations and metadata*](https://zenodo.org/records/10231865) | Historical adoption observations and published growth estimates | CSV/YAML, including [growth estimates](https://zenodo.org/records/10056128) |

## Registered input locations

Downloads and API responses live in `inputs/0_cache/`;
registered workbooks in [`inputs/0_external_models/`](../inputs/0_external_models/);
and reviewed excerpts in [`inputs/0_manual_params/`](../inputs/0_manual_params/).
Registered [*RAMP-mobility*](https://github.com/RAMP-project/RAMP-mobility) outputs and the compact [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) extract live under
[`inputs/0_external_models/`](../inputs/0_external_models/). The historical notebook
references live under `inputs/validation/transportation_reference/`. Backend-owned templates
are structural inputs with internal provenance rather than external-source citations.

## Download validation and recovery

The build checks registered identity before using pinned downloads, including files
placed manually in the cache. Source manifests record the bytes that the adapter
processed. An observed hash identifies the inputs to a run. Expected hashes come from
the [source registry](../config/sources.yaml) and protect subsequent runs even when no
manifest exists.

| Input | Registered identity | Where to find the observed checksum |
| --- | --- | --- |
| NRCan CEUD transport tables | The 2023 national and provincial workbooks each have a SHA-256 and byte count. Other explicitly selected editions without registry snapshots receive workbook and table-structure checks. | `inputs/1_interim/fetched_nrcan_ceud_inputs/manifest.csv`, one row per requested workbook |
| 2024 Transportation ATB | The official `2024_atb_transportation_data_v1.zip` has a registered SHA-256 and byte count. | `inputs/1_interim/fetched_nlr_atb_autonomie/manifest.csv`, with the ZIP identity on source member rows |
| Transport Canada iZEV | Each of the three registered CSVs is checked against its MD5 and byte count. | `inputs/1_interim/fetched_transport_canada_izev/izev_normalization_manifest.json`, with MD5, SHA-256 and bytes for each CSV |
| NRCan Dunsky report | The September 13, 2026 Government of Canada Web Archive capture contains the February 2024 report. The build checks the raw HTML against its registered SHA-256 and byte count. | `inputs/1_interim/dunsky/published_tables.json` |
| FuelEconomy.gov vehicles | The November 20, 2026 `vehicles.csv.zip` has a registered SHA-256 and byte count. | Source-updater report in `outputs/validation/source_updates/` or the adapter's manifest when its standalone normalization runs |

The manifests describe the latest completed preparation for that source. For a
failure before a manifest is written, use the timestamped build log in `outputs/logs/`.

When a cache file is missing, rerun the same scenario with `--download-sources`.
When a local artifact fails a checksum, keep it outside the configured cache path
while recovering the registered bytes. The download flag acquires missing files and
validates files already present. If the publisher has withdrawn or revised a pinned
file, recover the exact registered bytes from an audited copy or review a replacement
before changing the registry. Preserve the rejected bytes if they are needed to
diagnose the revision.

The [source updater](backend_architecture.md#explicit-source-updates) can inspect and
propose changes for the registered iZEV resources. CEUD and ATB are fixed-edition
registrations. To update one of them, review the publisher's replacement and its
table or archive compatibility, register the new checksum and a distinct cache path
where the bytes changed, and test fresh acquisition followed by offline replay. An
ATB change that affects vehicle archetype inputs also needs review against the
registered GREET results before building with embodied emissions enabled.

The clean rebuild also verified two publisher changes outside those three sources.
EPRI rebuilt the v2025 documentation and removed the previous JavaScript asset. The
registered replacement reproduces all 76 Intercity Bus chart rows. AFDC added an API
field to the December 31, 2025 historical response. The new pin preserves every previous
field and the normalized station and port totals. The new revisions have distinct cache
paths so existing files can still support earlier checkouts.

The Dunsky report's live page returned a JavaScript challenge to automated requests.
`--download-sources` retrieves the dated capture from Library and Archives Canada.
The cited NRCan publication URL remains the source link. All eight parsed tables,
29 production inputs and the diagnostic context match the previously registered HTML.
[Government of Canada Web Archive capture](https://webarchiveweb.wayback.bac-lac.canada.ca/web/20260913144437id_/https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada)

FuelEconomy.gov publishes a rolling archive. Use `--source fueleconomy_gov_vehicle_data`
to review changes to the classification, propulsion and range inputs exposed by the
current ZIP. If the publisher replaces the file, exact replay needs the ZIP that
matches the registry. Apply a captured review before switching to a newer snapshot,
then rebuild the affected scenarios. [Source updater](backend_architecture.md#explicit-source-updates)
