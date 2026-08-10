# Transportation source inventory

[The source registry](../config/sources.yaml) is authoritative for identity, version,
access, cache registration, citation, validation expectations, lifecycle status, and
reviewed data quality. This page is a concise discussion aid, not a second registry.
Families below consolidate their registered components.

Table counts are the number of registered `components`. Sizes are the measured
on-disk totals of registered raw inputs in the current workspace on 2026-08-07; they
exclude interim and processed outputs. A shared manual CSV is counted in each family
whose source contract registers that file. Fetch status describes acquisition or
registration only, not parameterization or SQLite insertion.

| Source family | Main transport use | Table count | Access method | Native format/artifacts | Input location | Size | Status |
| ------------- | ------------------ | ----------: | ------------- | ----------------------- | -------------- | ---: | ------------ |
| [NRCan Comprehensive Energy Use Database (provincial and national)](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) | Provincial road activity, stock, sales, distance, and fuel use; provincial non-road fuel use; national non-road activity, energy intensity, and fuel mix | 22 (17 provincial + 5 national) | Direct URL-templated downloads | Legacy XLS tables | `inputs/0_cache/` | 805.00 KiB | **Fetched** |
| [Ontario Ministry of Transportation vehicle population](https://data.ontario.ca/dataset/vehicle-population-data) | Report A LDV fleet-age weights, class-mapping evidence, and apparent-retention diagnostics; Report 4 MHDV weight classes; Report 5 legacy comparison only | 3 | Ontario CKAN package API with annual-resource discovery | Annual ZIP archives containing text reports | `inputs/0_cache/` | 22.88 MiB | **Fetched** |
| [Statistics Canada transportation tables](https://www.statcan.gc.ca/en/developers/wds) | LDV sales fuel shares, MD-truck registration fuel shares, HD-truck haul weights, and bus useful lives | 5 | Web Data Service metadata plus full-table download API | ZIP archives with CSV data and JSON metadata | `inputs/0_cache/` | 78.74 MiB | **Fetched** |
| [Canada Energy Regulator, Canada's Energy Future](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) | GDP growth for demand projection and macro factors for currency harmonization; transport end-use demand as benchmarking evidence; provisional cross-sector gasoline and diesel prices | 3 | Direct edition-specific Open Government CSV links | CSV files | `inputs/0_cache/` | 17.40 MiB | **Fetched** |
| [NLR Transportation Annual Technology Baseline 2024](https://atb.nlr.gov/transportation/2024/data) | Future road-vehicle efficiencies and prices, PHEV utility weighting, LDV maintenance assumptions, and age-based VMT schedules | 7 | Direct versioned ZIP download | ZIP containing source CSV and XLSX tables | `inputs/0_cache/` | 259.16 MiB | **Fetched** |
| [ANL Autonomie and BEAN 2022](https://anl.app.box.com/s/an4nx0v2xpudxtpsnkhd5peimzu4j1hk/folder/242640145714) | MHDV maintenance-and-repair linear-model coefficients | 1 | Registered manual Box-folder acquisition | XLSX/XLSM workbooks and PDF report | `inputs/0_external_models/` | 66.37 MiB | **Registered** |
| [NRCan Fuel Consumption Ratings](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64) | Existing LDV fuel/electricity consumption, range, and make-model class evidence used in road mapping | 6 | Direct Open Government resource downloads pinned by hash and byte count | CSV snapshots | `inputs/0_cache/` | 2.43 MiB | **Fetched** |
| [NHTSA CAFE 2024 Central Analysis](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/CAFE/2024-FRM-LD-2b3-2027-2035/Central-Analysis/) | LDV survival schedules and median-equivalent-lifetime evidence | 1 | Direct pinned ZIP download | ZIP containing Central Analysis workbook inputs | `inputs/0_cache/` | 8.79 MiB | **Fetched** |
| [EIA NEMS transportation inputs](https://github.com/EIAgov/NEMS/blob/main/input/tdm/trnhdvx.xlsx) | Medium/heavy-truck scrappage schedules and derived survival/lifetime evidence | 1 | Immutable Git LFS media at a pinned commit | XLSX workbook | `inputs/0_cache/` | 720.03 KiB | **Fetched** |
| [JGCRI GCAM core transport data](https://github.com/JGCRI/gcam-core/tree/master/input/gcamdata/inst/extdata/energy) | Canadian motorcycle energy intensity/efficiency, purchase cost, and maintenance cost | 1 | Immutable raw Git file at a pinned commit | CSV | `inputs/0_cache/` | 1.00 MiB | **Fetched** |
| [EPRI US-REGEN v2025 transportation](https://us-regen-docs.epri.com/v2025/assumptions/transportation.html) | Intercity-bus purchase cost and efficiency; reviewed non-road capital-cost and efficiency multipliers | 3 | VuePress page/payload snapshot plus registered manual CSV selectors | HTML, JavaScript payload, and CSV | `inputs/0_cache/`; `inputs/0_manual_params/` | 100.78 KiB | **Fetched + registered** |
| [FAA Economic Values 2024](https://www.faa.gov/regulations_policies/policy_guidance/benefit_cost) | Aircraft capacity, load factors, block speed, and maintenance cost used to normalize air capital and variable costs | 2 | Direct component PDF downloads | PDF report sections | `inputs/0_cache/` | 1.45 MiB | **Fetched** |
| [Wards Intelligence sales shares](https://wardsintelligence.informa.com/automotive-data) | Reviewed Canadian LDV class-share mapping retained for legacy road-aggregation comparison | 1 | Review-owned, version-controlled aggregate extraction | CSV | `inputs/0_manual_params/` | 10.23 KiB | **Registered** |
| [CIMS model assumptions](https://github.com/EMRG-SFU/cims/tree/main) | Service-output and service-unit capital-cost assumptions, plus process lifetimes for air, rail, and marine modes | 2 | Review-owned, version-controlled manual parameters | CSV | `inputs/0_manual_params/` | 12.47 KiB | **Registered** |
| [Open Energy Outlook](https://github.com/TemoaProject/oeo/tree/master/database_documentation) | Rail/marine operating-cost-to-capital-cost ratios and charging-infrastructure lifetime | 2 | Review-owned, version-controlled manual parameters | CSV | `inputs/0_manual_params/` | 5.51 KiB | **Registered** |
| [Argonne R&D GREET 2025 Rev.1](https://greet.anl.gov/greet/versions.html) | Reviewed marine HFO-to-MDO energy-intensity ratio | 1 | Review-owned, version-controlled manual parameter | CSV | `inputs/0_manual_params/` | 6.21 KiB | **Registered** |
| [EPA MOVES4 population and activity](https://www.epa.gov/system/files/documents/2023-08/420r23005.pdf) | Heavy-duty-truck process lifetime | 1 | Review-owned, version-controlled manual parameter | CSV | `inputs/0_manual_params/` | 3.48 KiB | **Registered** |
| [Canada Energy Policy Simulator v3.4.7](https://docs.energypolicy.solutions/models/canada) | Motorcycle process lifetime | 1 | Review-owned, version-controlled manual parameter | CSV | `inputs/0_manual_params/` | 3.48 KiB | **Registered** |
| [Argonne HDSAM v4.5](https://hdsam.es.anl.gov/index.php) | Hydrogen-refueling dispenser process lifetime | 1 | Review-owned, version-controlled manual parameter | CSV | `inputs/0_manual_params/` | 3.48 KiB | **Registered** |
| [FuelEconomy.gov vehicle data](https://www.fueleconomy.gov/feg/ws/index.shtml) | Additional make-model-year vehicle-class evidence for reviewed LDV mapping | 1 | Direct download pinned by hash and byte count | ZIP containing `vehicles.csv` | `inputs/0_cache/` | 2.08 MiB | **Fetched** |

## Boundary notes

- [Backend-owned templates](../inputs/0_canoe_template/) define structural technology,
  commodity, region, and period rows. They are not external sources and do not receive
  external citations, data-quality scores, or `Txx` identifiers.
- Authoritative downloads live under [`inputs/0_cache/`](../inputs/0_cache/), registered
  external-model artifacts under [`inputs/0_external_models/`](../inputs/0_external_models/),
  and reviewed manual tables under [`inputs/0_manual_params/`](../inputs/0_manual_params/).
  Normalized audit tables under [`inputs/1_interim/`](../inputs/1_interim/) are not
  counted as raw source footprint or proof of final parameter insertion.
- The national CEUD cache contains corrected component IDs 20, 21, 26, 27, and 28.
  The combined manifest records 22 cached tables (17 provincial and 5 national), the
  national normalized output contains 902 rows, and offline replay reproduced the
  normalized outputs without warnings.
- Registry lifecycle `active`/`inactive`, scenario activation, fetch status, and
  downstream parameter insertion are separate concepts.
