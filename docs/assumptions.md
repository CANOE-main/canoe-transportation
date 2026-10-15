---
title: CANOE-Transportation assumptions
role: Review record for source limitations, data gaps, modelling challenges, and their current handling.
retrieve_when: A task changes or discovers a transportation assumption, source limitation, data gap, or modelling challenge.
read_scope: Read only the affected parameter section and relevant rows.
verify: "Check current code, config, tests, and evidence; mark Codex-added or changed review content #to-review."
---

# CANOE-Transportation Assumptions

Each parameter section is presented as a single table. Rows are grouped by vehicle class, and assumptions that apply to multiple classes remain consolidated rather than duplicated.

`#to-clarify` tags are manually left on entries where there appears to be an error, the current implementation of those entries is left as context and evidence, where the tag explains what the correct approach should be.

## Shared road fleet composition

Configuration: [`aggregation_sources` selections](../config/scenarios/legacy_reproduction.yaml) and [`road_aggregation` rules](../config/parameters/rules.yaml). #to-review

Efficiencies, costs and prepared road mileage profiles share these class/vocation weights. LDV weights also aggregate NHTSA light-truck survival. Hold each mix across powertrains and vintages. #to-review

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Ontario MTO Report A is the available class-composition evidence; its reviewed crosswalk is incomplete | Pool mapped FIT_ACTIVE counts across model years in the selected Ontario edition and normalize over mapped classes. Treat this subset as representative across Canadian regions; passenger and freight light trucks share the same class mix. #to-review |
| Medium Trucks | Ontario fleet counts and national sales evidence cover different GVWR classes and lack vocation shares | Use MTO Report 4 Class 2b–7 counts in ON, mapping 2b to ATB Class 2; elsewhere use 2020 Wards Class 3–7 shares. Weight available freight vocations equally within class, excluding School/Transit/Refuse, including vocations with identical mileage schedules. #to-review |
| Heavy Trucks | StatCan 23-10-0142-01 for-hire shipments proxy regional cab-type use | Pool 2011–2017 origin-or-destination tonne-km, counting within-province shipments once. Retain shipment groups whose mean load plus a 13,000-kg curb weight reaches 14,970 kg. Distances up to 350 miles represent DayCab; longer distances represent Sleeper. Split DayCab weight equally among Beverage/Drayage/Regional vocations; use BC-only evidence for BCT. #to-review |

## `existing_capacity`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Ontario MTO is the available vehicle-population evidence | Treat latest mapped Report A FIT_ACTIVE ages as the CEUD stock-year age profile, even when the MTO edition is newer. Normalize eligible age shares to distribute full CEUD class totals across regions; unmapped records do not reduce those totals. #to-review |
| Existing LDV BEVs and PHEVs | Historical Canadian stock lacks range-specific sales weights | Always use one existing BEV and PHEV archetype per class, independently of the new-vehicle range mode. Retain registered stock totals and lifetime-limited cohort allocation; use OMEGA MY2022 range weights across historical vintages and regions for the corresponding parameters. #to-review |
| Cars and Light Trucks | StatCan LDV vehicle types differ from CEUD classes | Passenger-car fuel shares apply to cars; combined pickup, multipurpose-vehicle, and van shares apply to light-truck class.  |
| Cars and Light Trucks | CEUD and StatCan pickup weight bins differ | CEUD light trucks end at 8,500 lb; StatCan pickups extend to 14,000 lb. Treat the mismatch as negligible for fuel shares.  |
| Medium and Heavy Trucks | StatCan truck weight groups do not match CEUD exactly | The 10,001–33,000 lb groups proxy medium trucks and Class 8 proxies heavy trucks; 8,501–10,000 lb is uncovered.  |
| Cars and Trucks | StatCan fuel registrations start after some surviving vintages; some fuel types lack existing technologies | Earlier vintages repeat the oldest available gasoline/diesel shares after renormalization. Other unsupported shares are excluded and audited.  |
| Road Vehicles | StatCan LDV fuel registrations do not disclose AB and NL shares | BC shares proxy BCT. Canada-level shares proxy AB and NL only for LDVs; medium/heavy trucks use their provincial StatCan 23-10-0308-01 evidence. |
| Medium Trucks, Heavy Trucks, and Motorcycles | Ontario MTO Report 5 is the available age evidence | The latest COMMERCIAL ages proxy both truck classes; MOTORCYCLE ages proxy motorcycles across CEUD regions.  |
| Medium Trucks | Transport Canada's year-to-date medium/heavy EV share | Treat the reported share as medium-truck BEV stock in the latest existing vintage; split the remainder by StatCan gasoline/diesel proportions.  |
| Heavy Trucks and Motorcycles | Existing incumbent powertrains | Retain diesel heavy trucks and gasoline motorcycles only; exclude and audit non-diesel heavy-truck registration shares.  |
| Buses | CEUD bus stock powertrain disaggregation | Split latest CEUD stocks using each cohort year's CEUD fuel consumption times its powertrain efficiency. Map ethanol to gasoline and biodiesel to diesel; exclude propane with an audit.  |
| Buses | Ontario MTO Report 5 provides the available age profile | Apply its latest BUS ages to each bus class and region. Use 2000 fuel/activity evidence for older source ages, subject to the common first-period lifetime eligibility rule below. #to-review |
| Buses | Some historical fuel allocations have no cohort surviving to the first model period | Redistribute stock proportionally among eligible ages of the same technology. If none survives, transfer its stock proportionally to eligible technologies in the same region and bus class. #to-review |
| Air Transportation | NRCan CEUD aviation fuel use | Use aviation turbo fuel only; exclude and audit aviation gasoline.  |
| Passenger Rail | NRCan CEUD does not report electric passenger-rail energy | Build diesel passenger-rail capacity only; electric rail remains unrepresented.  |
| Air, Rail, and Marine | NRCan CEUD does not report provincial fleet stocks | Divide provincial energy use by same-year national energy intensity to estimate capacity in billion passenger-km or tonne-km.  |
| Air, Rail, and Marine | Annual turnover and CIMS model fixed lifetimes | First estimate each annual fleet total from CEUD energy divided by intensity. Earlier cohorts retain `max(0, 1 - age/lifetime)` of their effective starting capacity. Subtract these survivors to estimate additions, bounded below by zero. When survivors exceed the observed total, retire that excess proportionally across cohorts so their sum reconciles with the fleet estimate. #to-review |
| Marine Freight | CEUD reports separate HFO/MDO energy but one marine intensity | Use the shared marine intensity and combined fleet turnover, then split each active vintage by provincial 2023 HFO/MDO energy shares.  |
| Road and Off-road | Annual cohorts and configured existing periods | `period_mode`: `prospective` represents the following interval: 2021–2025 cohorts belong to vintage 2020, using observations through the latest CEUD vintage. `legacy` retains ceiling labels ending at the observation year. |
| Road and Off-road | Capacity must survive to the first model period | For fixed lifetimes, require both annual cohorts and their mapped model vintages to survive the first period. Redistribute retired cohorts' base-year capacity proportionally among eligible vintages of the same region and technology, with the bus recipient exception above. #to-review |
| LD and MHD EV Chargers | Existing private/MHD charger stocks are inferred from vehicle stocks | Estimate required ports from BEV stocks only, using 1 LD EV/port and 1.5 MHD EV/port. LD private ports equal required ports minus reported public ports; split MHD ports 80/20 between 50/350 kW. PHEV stocks do not enter this calibration. #to-review |
| LDEV Chargers | Transport Canada — undisclosed capacity vintage | Assign all reported capacity to the latest existing period. |

## `demand`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Air, Rail, and Marine | NRCan CEUD reports provincial energy but only national service intensity | Apply the same-year national intensity to each province's total mode energy to estimate provincial activity. Total air energy includes aviation gasoline, although existing incumbent capacity excludes it.  |
| All Transport Services | CER Canada's Energy Future macro indicators provide Canada-wide real GDP only | Apply national real GDP growth at period-end years to each province's CEUD service baseline, retaining the combined passenger-LDV total under the optional split below. Provincial GDP differences are unrepresented. #to-review |
| Cars and Passenger Light Trucks | National GDP does not determine their future service split | With `future_car_demand: extrapolated`, extend provincial car activity using the 2013–base-year endpoint CAGR (currently 2013–2023). Assign passenger light trucks the residual of the GDP-indexed combined total; reject car projections exceeding that total. `GDP-indexed` grows both services separately with GDP. #to-review |

## `limit_annual_capacity_factor`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | NRCan CEUD annual vehicle activity and stock; recent activity-to-stock ratios have declined | Use the mean of the latest five eligible annual activity-to-stock ratios, excluding 2020–2021, to establish each region and road class's utilization baseline.  |
| Cars and Trucks | NLR ATB age-based mileage profiles describe how Canadian utilization levels vary with age | Use each aggregated ATB profile only as a relative age trajectory, normalized to a peak of one; retain the CEUD-derived baseline as the utilization level.  |
| LDV range variants | Range sales weights do not describe vehicle utilization | Use the same annual utilization and capacity-to-activity factor for every powertrain/range within a region and vehicle class. Range aggregation weights parameter values; it does not alter utilization. Fixed lifetimes and survival curves also remain common across ranges. #to-review |
| Age-dependent road utilization | The pinned annual factor cannot index both vintage and operating period | Keep the prepared vintage/period age trajectories available for future schema support; the current database uses the common class-level annual baseline. #to-review |
| Air, Rail, and Marine | NRCan CEUD — no explicit fleet stocks | Utilization is not represented; UF = 1. |

## `capacity_to_activity`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | Manual `capacity_to_activity` values are arbitrary activity-per-vehicle scaling factors, not measured source values | Treat each road-class factor as a model scaling assumption shared across regions and powertrains.  |

## `lifetime_tech` and `lifetime_survival_curve`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Ontario registration changes include migration/re-registration and do not isolate physical scrappage; NHTSA CAFE supplies US survival schedules | Use reported cumulative car rates directly across regions. Combine Vans/SUVs and Pickups for both Canadian light-truck classes using the shared Ontario class weights. #to-review |
| Medium Trucks | NHTSA CAFE 2b/3 Trucks survival rates | Apply this cumulative survival schedule to every medium-truck GVWR class, powertrain, and region. #to-review |
| Heavy Trucks | NEMS national Class 7–8 scrappage table is marked unused upstream; MOVES5 Table C-1 supplies annual heavy-duty survival based on model-year-1980 evidence | Select either schedule as a common proxy across heavy-truck powertrains and regions. Compound NEMS retention `(1 − scrappage rate)` or MOVES5 annual survival directly. Fixed medians use the same selected curve: currently 25 years for NEMS and 19 for MOVES5. Other road classes retain NHTSA. #to-review |
| Cars and Trucks | Annual survival schedules must represent multi-year model periods | Use mean cumulative survival over each complete annual-age block: ages 0–4 for a new five-year cohort. Hold each class schedule unchanged across regions, powertrains and vintage years. #to-review |
| Road Vehicles | `period_mode`: `prospective` shifts surviving annual cohorts into earlier model vintages | Use supported ages from each selected schedule through 29 for the default prospective grid, supplying the complete ages 25–29 block for vintage 2000 in the first model period. #to-review |
| Cars and Trucks | Fixed lifetimes are derived from the selected survival schedules | With survival curves off, use the first annual age at which cumulative survival reaches 50% or less. #to-review |
| Buses | StatCan reports average public-transit asset lifetimes and suppresses some provincial cells | Apply the latest reported 2016–2020 value by powertrain to all three bus classes. For BCT, use BC first; use Canada 2020 only when that province or proxy has no reported value for the powertrain. |
| Buses | StatCan has no gasoline, plug-in hybrid, or fuel-cell bus useful-life series | Use diesel bus lifetimes for gasoline buses and electric bus lifetimes for plug-in hybrid and fuel-cell buses. |
| Other Road, Off-road, and Infrastructure Technologies | Reviewed manual lifetime evidence is at vehicle or equipment category level | Apply each reviewed category lifetime across its technologies and regions where no survival curve or StatCan lifetime supplies that technology. |

## `efficiency`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | CEUD activity, annual distance, and stock describe whole vehicle classes, not individual powertrains | Derive class occupancy/payload as activity divided by distance and stock; apply it across powertrains and hold the base-year factor constant for future vintages. #to-review |
| Cars, Trucks and PHEV Blends | Fuel-volume ratings and electrical consumption need a common energy basis | Convert gasoline/diesel volume consumption with GREET higher heating values and electricity with 3.6 MJ/kWh when deriving model efficiencies and PHEV input-energy fractions. See the [conversion factors](../config/parameters/conversion.yaml). #to-review |
| Road and Off-road | Historical evidence ends before the latest prospective interval ends | Average available annual efficiency values within each selected historical bin, stopping at the observation year. Future efficiencies use period-end projections in both prospective and legacy modes; the incomplete 2020 interval uses observed 2021–2023 values. #to-review |
| Cars and Light Trucks | NRCan model-year ratings are not measurements of the surviving fleet's in-use consumption | Treat ratings as vintage-specific fleet efficiency proxies, without a separate aging adjustment; weight rating observations equally within each class and powertrain. #to-review |
| Cars and Light Trucks | Historical ratings combine SUV sizes and omit some class/powertrain combinations | Give unsplit SUVs the combined small/standard SUV weight; redistribute missing-class weights only across observed classes. #to-review |
| Cars and Light Trucks | NRCan ratings lack a complete HEV flag | Identify hybrids by model-name wording or unambiguous year/make/model matches to US FuelEconomy.gov hybrid records. Otherwise retain the reported fuel classification, which can leave unidentified HEVs among conventional vehicles. #to-review |
| Cars and Light Trucks | NRCan electric ranges do not coincide with ATB archetypes | Classify BEVs with `Range (km)` and PHEVs with `Range 1 (km)`, converted to miles, using the same OMEGA range-bucket boundaries as the sales weights. Use all four BEV and both PHEV buckets before existing-fleet aggregation. #to-review |
| Existing LDV BEVs and PHEVs | A single stock archetype still requires range-specific efficiency evidence | Derive each NRCan range bucket's historical efficiency before OMEGA aggregation. Retain the accepted ATB utility-weighted total-energy level for each PHEV range and its NRCan charge-sustaining historical index. Preserve configured analogue fallbacks when a range lacks observations; do not drop that range or renormalize its sales weight. #to-review |
| Existing LDV BEVs and PHEVs | Registered NRCan range histories begin at different years | Car BEV150/200/300/400 observations begin in 2012/2013/2013/2019; PHEV35/50 begin in 2012/2013. Light-truck BEV200/300/400 begin in 2016/2016/2020; PHEV35/50 begin in 2015/2020. Earlier required years use the configured historical analogues. Light-truck BEV150 has no rating observations and zero OMEGA weight. These are source-history gaps, not unfilled final parameter rows. #to-review |
| Cars and Light Trucks | Some powertrains lack continuous historical ratings | Retain the latest own observation and extend it with the approved analogue's historical index. Where no own baseline exists, use the analogue's level scaled by the ATB powertrain relationship. #to-review |
| Cars and Light Trucks | The [2024 ATB archive](https://atb.nlr.gov/transportation/2024/data) offers different hybrid architectures at different electric ranges | Represent full gasoline HEVs with power-split and 35/50-mile PHEVs with Voltec EREV powertrains. EREV is the only ATB PHEV architecture matching those two modeled ranges; its power-split PHEV is 30 miles. The original Autonomie results additionally offered parallel PHEV35/50 variants selected by the supplied legacy workbook. #to-review |
| Cars and Light Trucks | NLR ATB light-duty hybrid diesel electric powertrains are neglected | LDV hybrid electric powertrains that utilize diesel are neglected; it is assumed that gasoline-powered HEVs will remain the dominant variant for the light-duty sector.  |
| Cars and Light Trucks | US ATB scenarios provide projected technology improvements, not Canadian fleet observations | Apply ATB relative improvements to Canadian rating-based baselines; do not substitute ATB absolute levels except for PHEVs or missing-baseline analogues. #to-review |
| PHEVs | ATB fleet utility factors describe distance in charge-depleting operation | Represent charge-depleting travel by electricity consumption and charge-sustaining travel by liquid-fuel consumption. Derive input shares from these utility-weighted energy components. #to-review |
| PHEVs | Fuel/electricity shares differ across ATB classes and regional truck compositions | For ordinary LDV35/LDV50 blends use pooled MTO Report A class stock counts; for MDV and HDV blends use the selected regional GVWR/vocation and haul weights. Sum weighted electrical and total consumption before taking their ratio, then average future interval-end fractions equally. Hold each regional blend constant across periods; buses retain the HDV blend. Existing LDV representatives have separate category-specific blends with their class and OMEGA range weights. #to-review |
| Medium/Heavy Trucks and Buses | Selected ATB evidence lacks gasoline and CNG archetypes; the original Islam et al. (2023) results used by legacy included separate gasoline MDV parameters | Use the diesel incumbent as their ATB efficiency proxy; MDV gasoline retains its fuel-specific CEUD historical backcasting index. Intercity CNG instead uses the REGEN relative relationship. #to-review |
| Medium and Heavy Trucks | Historical CEUD fuel consumption describes the fleet rather than individual model years | Use its gasoline/diesel trajectory as a vintage backcasting index from the ATB baseline, with higher consumption implying lower efficiency. #to-review |
| Medium and Heavy Trucks | ANL Autonomie, as incorporated in NLR ATB — PHEV utility-weighted combined efficiency | Islam et al. (2023) assume a utility factor of 80%. |
| Buses | CEUD historical intensity is class-specific but not powertrain-specific | Apply each bus class's intensity trend across its existing powertrains; use ATB school/transit levels for future vehicles, weighting the two school-bus classes equally. #to-review |
| Intercity Buses | ATB transit buses and REGEN relative efficiencies proxy intercity technology performance | Use transit-bus efficiency levels with REGEN intercity powertrain ratios; retain the ATB transit HEV relationship where REGEN has no HEV series. #to-review |
| Motorcycles | GCAM Canada supplies technology trajectories rather than provincial motorcycle observations | Apply GCAM gasoline/BEV intensity relationships to each province's CEUD motorcycle baseline. #to-review |
| Air, Rail, and Marine | CEUD service intensities are national mode averages, not province- or powertrain-specific | Apply the national historical intensity across provinces as the incumbent baseline; use reviewed relative multipliers for alternative powertrains. #to-review |
| Off-road Modes | Reviewed EPRI US-REGEN assumptions proxy Canadian future performance | Apply the same relative technology factors and compound annual efficiency improvements across provinces; interpolate hydrogen-rail ratios between the reviewed 2035 and 2050 values. #to-review |
| Passenger and Freight Rail | REGEN natural-gas rail assumptions describe CNG, not LNG | Use the CNG-to-diesel efficiency relationship for LNG rail technologies. #to-review |
| Marine Freight | CEUD reports one marine intensity; GREET's HFO/MDO ratio describes a predefined trip | Treat CEUD intensity as the MDO baseline and apply the GREET HFO/MDO consumption ratio across historical and future vessels, regions, and vintages. #to-review |
| Aviation | No separate SPK aircraft-efficiency relationship is used | Assign SPK aircraft the same service-per-energy efficiency and improvement rate as jet-fuel aircraft. #to-review |

### Example LDV Class Mapping by Source

| **Vehicle Model** | **NRCan Fuel Ratings** | **Autonomie TEA** | **NRCan CEUD** |
| ----------------- | ---------------------- | ----------------- | -------------- |
| **Nissan Sentra** | Midsize                | Midsize           | Car            |
| **Nissan Versa**  | Compact                | Compact           | Car            |
| **Toyota RAV4**   | Small SUV              | Small SUV         | Light Truck    |
| **Ford F-150**    | Standard pickup truck  | Pickup truck      | Light Truck    |
| **Ford Explorer** | Standard SUV           | Midsize SUV       | Light Truck    |

## `cost_invest`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | NLR ATB vehicle prices include a source-family retail-price-equivalent markup | Divide selected retail purchase prices by 1.5 for LDVs and 1.2 for MHDVs, following [ATB definitions](https://atb.nlr.gov/transportation/2024/definitions), and preserve original retail MSRP for repair calculations. These factors describe Mid/Advanced Autonomie-derived prices; using them for Conservative AEO retail prices is a manufacturing-cost proxy. Do not apply ATB markups to other capital-cost sources. #to-review |
| LDV FCEVs | Lower ATB fuel-cell and hydrogen-storage cost coefficients drive the price reduction versus Islam et al. (2023) | Retain the [ATB coefficients](https://atb.nlr.gov/transportation/2024/fuel_cell_electric_vehicle_assumptions). In 2025 Mid source data, FCEVs cost less than BEV400s across all LDV classes, reversing Islam's ranking. #to-review |
| Transport Technologies | Projected cost trajectories and historical evidence have different temporal coverage | Prospective future costs use end-of-period conditions: period 2025 reads 2030 values. Historical year-varying cost evidence uses the bin's last observed year, retaining earliest-available price proxies and manual baseline conventions. Legacy preserves the prior adapter timing. Dollar-year conversion remains independent. #to-review |
| Medium Trucks and Buses | Selected ATB prices lack gasoline/CNG MHDV archetypes; original Islam et al. (2023) results used by legacy included separate gasoline SI series | Use the ATB diesel purchase price for gasoline/CNG medium trucks and school/transit buses. This is a vehicle-price proxy; BEAN maintenance coefficients and fuel-specific historical efficiency indices retain their own source treatment. #to-review |
| Intercity buses | REGEN Chart 3 has ICEV, CNGV, BEV, and HFCV purchase prices but no HEV | Use ICEV for gasoline/diesel and apply the NLR transit-bus HEV/diesel ratio to REGEN ICEV for intercity HEV. Preserve the REGEN purchase-price basis without an ATB markup adjustment. #to-review |
| Aircraft | FAA reports aggregate passenger and cargo operating evidence; cargo capacity is labelled only tons | Use each applicable All Aircraft average for speed, capacity, load, and daily utilization. Interpret cargo tons as U.S. short tons and annualize service output over 365 days. SPK uses jet-fuel aircraft capital cost. #to-review |
| Rail and Marine | REGEN supplies relative 2035/2050 capital-cost factors against reviewed CIMS incumbent baselines | Apply each factor to its corresponding CIMS baseline; interpolate between 2035 and 2050 and hold the 2035 factor earlier. #to-review |
| Marine Freight | The reviewed manual HFO capital-cost factor has no external citation | Treat HFO and MDO marine freight capital costs as equal. #to-review |
| Cost source metadata | Some sources omit currency or dollar-year evidence | Source-documented ATB vehicle prices are 2022 USD and ATB/Burnham LDV M&R coefficients are 2020 USD. Preliminary entries remain BEAN MHDV M&R: 2020 USD (year unspecified); GCAM motorcycles: 2005 USD (currency unspecified); REGEN intercity purchases: 2025 USD (currency and price year unspecified); FAA maintenance: 2023 USD (price year unspecified). Keep all entries and their status in the cost metadata audit; manual CIMS/OEO and charger currency/year declarations remain visible in their input rows and conversion evidence. #to-review |

## `cost_variable`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Burnham et al. (2021) and Islam et al. (2022) — empirical model coefficients are based on US mileage profiles | The same empirical-model coefficients used in these studies are applied to estimate maintenance and repair (M&R) costs per mile by vehicle age. |
| Cars and Light Trucks | ATB MSRP prices begin in 2023; Burnham's repair relationship uses a 2020-dollar MSRP basis and its age coefficients end at 14 | Use the selected retail MSRP with the original calibrated coefficients despite the dollar-basis difference. Proxy earlier vintages with the first ATB MSRP and hold repair cost constant after age 14. #to-review |
| Road Vehicles | Age-dependent maintenance and partial historical intervals | Prospective maintenance uses the period-end year relative to the vintage's evidence year; the latest historical bin ends at the observed year rather than an unobserved future endpoint. Legacy retains period-label minus vintage-label ages. #to-review |
| Medium and Heavy Trucks | BEAN lacks coefficients for several modeled GVWR classes | Proxy GVWR classes 2–5 with BEAN Class 4 and classes 6–7 with Class 6; retain the reviewed road class weights. #to-review |
| School and Intercity Buses | Their own maintenance evidence is incomplete | Use BEAN Class 8 transit maintenance and the transit-bus maintenance-to-purchase-cost relationship, scaled to each bus purchase cost. #to-review |
| Rail and Marine | OEO gives class-specific operating-to-capital-cost relationships rather than complete technology-specific variable costs | Apply each reviewed class ratio to its corresponding CIMS capital-cost baseline for existing and new vintages. #to-review |
| Passenger and Freight Aircraft | FAA supplies aggregate maintenance cost per block-hour rather than Canadian service-distance costs | Use the applicable All Aircraft maintenance cost with its passenger or cargo speed, capacity, and load evidence; keep the two service types separate. SPK uses jet-fuel aircraft operating cost. #to-review |

## EV charger parameters

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| LDV chargers | TC provincial public port counts are March 2026 while the prepared BEV stock is for 2023 | Apply the pinned March 2026 public counts to 2023 BEV stock when estimating private ports; record both dates in the charger audit. #to-review |
| LDV chargers in BCT | TC separates British Columbia and the territories; CANOE's BCT region groups them | Sum British Columbia, Yukon, Northwest Territories, and Nunavut public ports before estimating BCT charger capacity. #to-review |
| Existing LDV chargers | Provincial public counts do not specify charger-type composition | Apply national TC L2/DCFC shares to each region's public ports and a 15/85 L1/L2 mix to private ports. Convert counts to GW using 1.5/7.2/50 kW for L1/L2/DCFC in every region. #to-review |
| LDV and MHDV chargers | Reviewed efficiencies simplify charger-type and operating-load differences | Apply constant 95% LDV and 80% MHDV charging efficiency across regions, vintages and periods, independent of charger type; see the [reviewed charger parameters](../inputs/0_manual_params/charger_parameters.csv). #to-review |
| Existing and new chargers | Reviewed costs begin in 2025, while existing capacity is calibrated to 2023 stocks | Use the earliest reviewed 2025 cost rates for existing chargers' annual fixed costs, with vintage 2020 in prospective mode or 2023 in the prior legacy grid. Only new chargers receive investment rows. Future costs read vintage interval-end years in prospective mode and vintage labels in legacy mode. Apply the same reviewed national port-count shares to per-GW costs across regions/vintages and the OEO 1% fixed-cost ratio. #to-review |
| Charger utilization | The pinned annual capacity factor is indexed by vintage rather than calendar period | Insert vintage-based upper bounds: LDV historical chargers retain the earliest reviewed 2025 factor of 0.15; new LDV cohorts use 0.15 for a 2025 input year and 0.20 thereafter. MHDV factors are 0.30. Prospective new vintage 2025 reads the 2030 input year; legacy vintage 2025 reads 2025. A factor stays with its cohort throughout its lifetime. #to-review |

## `emission_embodied`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| New road vehicles | GREET vehicle-cycle results are lifetime totals per manufactured vehicle | Apply only to new technologies ending in `_N`; existing vehicles inherit no manufacturing emissions. Use individual gas totals without lifetime, mileage or annual-rate division; exclude the combined CO2 measure. #to-review |
| CO2, CH4 and N2O | Emission layers require consistent gas-specific mass units | Vehicle capacity is in thousands; report CO2 as `kt/k vehicles` and CH4/N2O as `t/k vehicles`. Unit scaling preserves physical mass. Operating factors, emissions limits and CO2e calculations must respect these gas-specific units. #to-review |
| Cars and trucks | GREET vehicle-cycle scope differs from fuel-cycle and operating emissions | Include material production/processing, vehicle manufacturing/assembly and end-of-life decommissioning without recycling credits. Transport of raw and processed materials is neglected, following the vehicle-cycle scope in the flowchart. #to-review |
| Cars and trucks | A single GREET simulation year supplies vehicle-cycle factors | Hold the 2025 simulation's factors constant through 2050; its imported LDV vehicle-cohort year is 2020. #to-review |
| Cars and light trucks | GREET PHEV/BEV battery ranges and vehicle classes differ | Use the actual ATB technology ranges: PHEV 35/50 miles and BEV 150/200/300/400 miles. Map cars directly; retain the established pickup and SUV weights for light trucks and renormalize them proportionately over those groups. Gasoline, diesel and CNG use the ICEV vehicle-cycle block. #to-review |
| Medium and heavy trucks | GREET offers Class 6 and Class 8 Day/Sleeper vehicle-cycle models | Class 6 represents medium trucks while preserving scenario-selected GVWR weights. Aggregate heavy trucks with existing regional DayCab/Sleeper haul weights. HEV/PHEV share the HEV block and FCEV/FCHEV share the FCV block; MHDV factors have no material scenario and do not depend on LDV selectors. #to-review |
| LDV lightweight BEVs | The registered GREET2 glider switch selects ranges inconsistent with its adjacent labels | For AER 150/200/300/400, the native switch selects EV400/EV150/EV200/EV300 gliders. W4/Y4/AA4/AC4 contain identical inputs within each of Car, SUV and PUT in the registered release, so this mismatch has no numerical effect and lightweight factors are accepted. Preserve the native formula and automated range selectors. Recheck saved inputs on source refresh and require correction of the downstream selector if range assumptions diverge. #to-review |

## `limit_new_capacity_share` and representative LDV ranges

Configuration: [`epa_omega_baseline` source](../config/sources.yaml), [`ldv_ev_ranges` rules](../config/parameters/rules.yaml), and [`BEV_PHEV_range_representation` selection](../config/scenarios/legacy_reproduction.yaml). #to-review

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | OMEGA U.S. MY2022 baseline is model evidence, not current Canadian sales | Use sales-weighted charge-depleting range distributions within each class and powertrain. These are range shares, not BEV/PHEV adoption shares. #to-review |
| Cars and Light Trucks | The U.S. snapshot lacks Canadian and future-specific market weights | Apply the car mix to Canadian cars and the light-truck mix to both passenger and freight light trucks. Hold shares constant across regions and future vintages. #to-review |
| New LDV BEVs and PHEVs | Cheapest short-range options would otherwise dominate deployment | `new_capacity_shares` retains all vehicle variants and their parameters, adding tech groups, membership, and constant OMEGA minimum shares (`>=`) within each category/powertrain family. Because complete bucket shares sum to one, the bounds fix that family's range mix. They impose no BEV/PHEV adoption share. #to-review |
| New LDV BEVs and PHEVs | `representative_archetype` combines range variants | Replace each BEV/PHEV family with one archetype before insertion. Weight consumption, costs, and lifetime embodied emissions by the same OMEGA sales mix; derive efficiency from weighted consumption. Emit no new-capacity-share constraints or range groups. #to-review |
| Representative LDV PHEVs | The supporting gasoline/electricity split cannot vary by vintage | Average each category's consumption-weighted energy shares equally across model vintages and apply the result to all vintages and periods. Total energy is conserved; component fuel use is approximate. #to-review |

## `capacity_factor_tech` for LDV charging profiles

Configuration: [`legacy_charging_profiles` source](../config/sources.yaml), [`legacy_charging_profiles` / `ldv_charging_profiles` rules](../config/parameters/rules.yaml), and [`charging_profiles` selection](../config/scenarios/legacy_reproduction.yaml). #to-review

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | StatCan Table 23-10-0308-01 — registered vehicle size class shares | Shares are irrespective of powertrain type given that charging profiles are also meant to represent future vehicle compositions, not just present-day fleets. |
| Cars and Light Trucks | StatCan 2021 Census — occupation demographics by province | Occupation shares between workers, students, and inactive individuals aged 15+ in private households do not differentiate between drivers and non-drivers |
| Shared LDV chargers | Legacy RAMP-mobility outputs describe an Ontario BEV fleet | Use the selected Ontario shape unchanged in every configured region on existing and new shared LDV chargers, including PHEV and motorcycle charging. #to-review |
| LDV charging composition | Fleet, range shares and charger mix are already embedded in the profiles | Keep the inherited composition across model periods. It may differ from the OMEGA market mix; applying new range shares to the profile again would double-count them. #to-review |
| TTS charging selection | The inherited TTS simulation uses weekday travel only | Accept the frozen weekday-based behaviour without reconstructing weekend travel. #to-review |
| LDV charging timing | The reference year uses Toronto time and includes DST transitions | Reuse the peak-normalized 2018 hourly shape across model years. Elapsed-hour labels preserve physical hours through DST without province-specific timezone shifts; `time_mapping` projects onto CANOE slices rather than changing the reference timezone. #to-review |
