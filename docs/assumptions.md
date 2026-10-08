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

## `existing_capacity`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Ontario MTO is the available vehicle-population evidence | Mapped latest Report A age shares distribute latest NRCan CEUD stocks in every CEUD region.  |
| Existing LDV BEVs and PHEVs | The old template assigned the entire stock to one Ontario-era range and omitted freight light-truck BEVs | Always use one existing BEV and PHEV archetype per class, independently of the new-vehicle range mode. Retain registered stock totals and lifetime-limited cohort allocation; use OMEGA MY2022 range weights across historical vintages and regions for the corresponding parameters. #to-review |
| Cars and Light Trucks | StatCan LDV vehicle types differ from CEUD classes | Passenger-car fuel shares apply to cars; combined pickup, multipurpose-vehicle, and van shares apply to light-truck class.  |
| Cars and Light Trucks | CEUD and StatCan pickup weight bins differ | CEUD light trucks end at 8,500 lb; StatCan pickups extend to 14,000 lb. Treat the mismatch as negligible for fuel shares.  |
| Medium and Heavy Trucks | StatCan truck weight groups do not match CEUD exactly | The 10,001–33,000 lb groups proxy medium trucks and Class 8 proxies heavy trucks; 8,501–10,000 lb is uncovered.  |
| Cars and Trucks | StatCan fuel registrations start after some surviving vintages; some fuel types lack existing technologies | Earlier vintages repeat the oldest available gasoline/diesel shares after renormalization. Other unsupported shares are excluded and audited.  |
| Road Vehicles | StatCan LDV fuel registrations do not disclose AB and NL shares | BC shares proxy BCT. Canada-level shares proxy AB and NL only for LDVs; medium/heavy trucks use their provincial StatCan 23-10-0308-01 evidence. |
| Medium Trucks, Heavy Trucks, and Motorcycles | Ontario MTO Report 5 is the available age evidence | The latest COMMERCIAL ages proxy both truck classes; MOTORCYCLE ages proxy motorcycles across CEUD regions.  |
| Medium Trucks | Transport Canada's year-to-date medium/heavy EV share | Treat the reported share as medium-truck BEV stock in the latest existing vintage; split the remainder by StatCan gasoline/diesel proportions.  |
| Road Vehicles | Fixed-lifetime vintage eligibility | Cars/light trucks use medians derived from accepted NHTSA CAFE curves; all medium trucks use the NHTSA CAFE 2b/3 Trucks median; heavy trucks/motorcycles use reviewed manual lifetimes. When fixed lifetimes apply, require both the annual cohort and its mapped model vintage to survive the first model period, redistributing stock over eligible cohorts. #to-review |
| Heavy Trucks and Motorcycles | Existing incumbent powertrains | Retain diesel heavy trucks and gasoline motorcycles only; exclude and audit non-diesel heavy-truck registration shares.  |
| Buses | CEUD bus stock powertrain disaggregation | Split latest CEUD stocks using each cohort year's CEUD fuel consumption times its powertrain efficiency. Map ethanol to gasoline and biodiesel to diesel; exclude propane with an audit.  |
| Buses | Ontario MTO Report 5 provides the available age profile | Apply its latest BUS ages to each bus class and region. Use 2000 fuel/activity evidence for older source ages, but require both the annual cohort and its mapped model vintage to survive the StatCan fixed lifetime at the first model period. Redistribute stock over eligible ages and vintages to preserve the class total. #to-review |
| Buses | Some historical fuel allocations have no cohort surviving to the first model period | Exclude ineligible ages and redistribute their stock proportionally across eligible ages of the same technology. If no age of that technology survives, retain the audited proportional transfer to eligible technologies in the same region and bus class. Old fuel shares do not create an over-age surviving cohort. #to-review |
| Air Transportation | NRCan CEUD aviation fuel use | Use aviation turbo fuel only; exclude and audit aviation gasoline.  |
| Passenger Rail | NRCan CEUD does not report electric passenger-rail energy | Build diesel passenger-rail capacity only; electric rail remains unrepresented.  |
| Air, Rail, and Marine | NRCan CEUD does not report provincial fleet stocks | Divide provincial energy use by same-year national energy intensity to estimate capacity in billion passenger-km or tonne-km.  |
| Air, Rail, and Marine | Annual turnover and CIMS model fixed lifetimes | First estimate each annual fleet total from CEUD energy divided by intensity. Earlier cohorts retain `max(0, 1 - age/lifetime)` of their effective starting capacity. Subtract these survivors to estimate additions, bounded below by zero. When survivors exceed the observed total, retire that excess proportionally across cohorts so their sum reconciles with the fleet estimate. #to-review |
| Marine Freight | CEUD reports separate HFO/MDO energy but one marine intensity | Use the shared marine intensity and combined fleet turnover, then split each active vintage by provincial 2023 HFO/MDO energy shares.  |
| Road and Off-road | Annual cohorts and configured existing periods | `period_mode`: `prospective` represents the following interval: 2021–2025 cohorts belong to vintage 2020, using observations through the latest CEUD vintage. `legacy` retains ceiling labels ending at the observation year. |
| Road and Off-road | Capacity must survive to the first model period | For fixed lifetimes, require both annual cohorts and their mapped model vintages to survive the first period. Redistribute retired cohorts' base-year capacity proportionally among eligible vintages of the same region and technology. Accepted road survival curves cover cars, light trucks, medium trucks, and heavy trucks. #to-review |
| LD and MHD EV Chargers | NRCan/Dunsky — unaccounted existing private charger capacity | Dunsky's 1 LD EV/port and 1.5 MHD EV/port ratios estimate private ports; NRCan type shares distribute LD ports. Use the reviewed MHD 50/350-kW shares for existing capacity; the manual utilization note mentioning 100 kW does not govern that calculation. #to-review |
| LDEV Chargers | Transport Canada — undisclosed capacity vintage | Assign all reported capacity to the latest existing period. |

## `demand`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Air, Rail, and Marine | NRCan CEUD reports provincial energy but only national service intensity | Apply the same-year national intensity to each province's total mode energy to estimate provincial activity. Total air energy includes aviation gasoline, although existing incumbent capacity excludes it.  |
| All Transport Services | CER Canada's Energy Future macro indicators provide Canada-wide real GDP only | Apply national real GDP growth to each province's own CEUD service baseline; provincial GDP differences are not represented.  |

## `limit_annual_capacity_factor`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | NRCan CEUD annual vehicle activity and stock; recent activity-to-stock ratios have declined | Use the mean of the latest five eligible annual activity-to-stock ratios, excluding 2020–2021, to establish each region and road class's utilization baseline.  |
| Cars and Trucks | NLR ATB age-based mileage profiles describe how Canadian utilization levels vary with age | Use each aggregated ATB profile only as a relative age trajectory, normalized to a peak of one; retain the CEUD-derived baseline as the utilization level.  |
| LDV range variants | Range sales weights do not describe vehicle utilization | Use the same annual utilization and capacity-to-activity factor for every powertrain/range within a region and vehicle class. Range aggregation weights parameter values; it does not alter utilization. Fixed lifetimes and survival curves also remain common across ranges. #to-review |
| Cars and Light Trucks | Ontario MTO vehicle-population evidence supplies the reviewed ATB class weights | Apply the all-vintage Ontario weights across CEUD regions; passenger and freight light trucks share the resulting Light Truck age pattern.  |
| Medium Trucks | Available fleet weights differ between Ontario and other regions; ATB has several freight vocations within each weight class | Use MTO Report 4 weights in ON, including Class 2b mapped to the ATB Class 2 VMT schedule; elsewhere use 2020 Wards Class 3–7 weights, excluding Class 2/2b. Weight available vocations equally within each class, excluding School/Transit/Refuse, even when their mileage schedules are identical.  |
| Heavy Trucks | StatCan Table 23-10-0142-01 for-hire shipment activity proxies provincial cab-type use | Pool 2011–2017 tonne-km for shipments whose origin or destination belongs to each province, counting within-province shipments once. Use regional/long-haul shares for DayCab/Sleeper, with equal weights across Beverage, Drayage, and Regional DayCab vocations, shared by efficiencies, costs, and utilization. Use BC-only evidence for BCT, giving BC precedence over territory samples. |
| Age-dependent road utilization | The pinned annual factor cannot index both vintage and operating period | Keep the prepared vintage/period age trajectories available for future schema support; the current database uses the common class-level annual baseline. #to-review |
| Air, Rail, and Marine | NRCan CEUD — no explicit fleet stocks | Utilization is not represented; UF = 1. |

## `capacity_to_activity`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | Manual `capacity_to_activity` values are arbitrary activity-per-vehicle scaling factors, not measured source values | Treat each road-class factor as a model scaling assumption shared across regions and powertrains.  |

## `lifetime_tech` and `lifetime_survival_curve`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | US NHTSA CAFE survival rates proxy Ontario vehicle survival | Use reported cumulative car rates directly on all regions. Combine Vans/SUVs and Pickups for both Canadian light-truck classes using the latest MTO Report A class weights across all regions, consistent with efficiencies, costs, and utilization. |
| Medium Trucks | NHTSA CAFE 2b/3 Trucks survival rates  | Apply this cumulative survival schedule to every medium-truck GVWR class, powertrain, and region. Report 4/Wards fleet weights remain shared across parameters. |
| Heavy Trucks | Reviewed fixed lifetimes and NEMS scrappage rates provide different lifetime representations | With survival curves off, heavy-truck fixed lifetime fetched from `inputs/0_manual_params/lifetime_process.csv`. With curves on, NEMS Class 7–8 annual scrappage rates used to derive survival rates. |
| Road Vehicles | `period_mode`: `prospective` shifts surviving annual cohorts into earlier model vintages | Use supported NHTSA/NEMS ages through 29 for the default prospective grid, supplying the complete ages 25–29 block for vintage 2000 in the first model period. |
| Cars, Light Trucks, and Medium Trucks | Their fixed lifetimes are derived from accepted survival schedules | Use the first annual age at which cumulative survival reaches 50% or less as the fixed lifetime.  |
| Buses | StatCan reports average public-transit asset lifetimes and suppresses some provincial cells | Apply the latest reported 2016–2020 value by powertrain to all three bus classes. For BCT, use BC first; use Canada 2020 only when that province or proxy has no reported value for the powertrain. |
| Buses | StatCan has no gasoline, plug-in hybrid, or fuel-cell bus useful-life series | Use diesel bus lifetimes for gasoline buses and electric bus lifetimes for plug-in hybrid and fuel-cell buses. |
| Other Road, Off-road, and Infrastructure Technologies | Reviewed manual lifetime evidence is at vehicle or equipment category level | Apply each reviewed category lifetime across its technologies and regions where no survival curve or StatCan lifetime supplies that technology. |

## `efficiency`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | CEUD activity, annual distance, and stock describe whole vehicle classes, not individual powertrains | Derive class occupancy/payload as activity divided by distance and stock; apply it across powertrains and hold the base-year factor constant for future vintages. #to-review |
| Road and Off-road | Historical evidence ends before the latest prospective interval ends | Average available annual efficiency values within each selected historical bin, stopping at the observation year. Future efficiencies use period-end projections in both prospective and legacy modes; the incomplete 2020 interval uses observed 2021–2023 values. #to-review |
| Cars and Light Trucks | NRCan model-year ratings are not measurements of the surviving fleet's in-use consumption | Treat ratings as vintage-specific fleet efficiency proxies, without a separate aging adjustment; weight rating observations equally within each class and powertrain. #to-review |
| Cars and Light Trucks | Reviewed Ontario MTO class shares are not province-, powertrain-, or future-specific | Apply all-vintage Ontario class weights across provinces, powertrains, and vintages; passenger and freight light trucks share the same vehicle-class mix. #to-review |
| Cars and Light Trucks | Historical ratings combine SUV sizes and omit some class/powertrain combinations | Give unsplit SUVs the combined small/standard SUV weight; redistribute missing-class weights only across observed classes. #to-review |
| Cars and Light Trucks | NRCan ratings lack a complete HEV flag | Identify hybrids by model-name wording or unambiguous year/make/model matches to US FuelEconomy.gov hybrid records. Otherwise retain the reported fuel classification, which can leave unidentified HEVs among conventional vehicles. #to-review |
| Cars and Light Trucks | NRCan electric ranges do not coincide with ATB archetypes | Classify BEVs with `Range (km)` and PHEVs with `Range 1 (km)`, converted to miles, using the same OMEGA range-bucket boundaries as the sales weights. Preserve native ranges and the former three BEV rating-band labels for audit; use all four BEV and both PHEV buckets before existing-fleet aggregation. #to-review |
| Existing LDV BEVs and PHEVs | A single stock archetype still requires range-specific efficiency evidence | Derive each NRCan range bucket's historical efficiency before OMEGA aggregation. Retain the accepted ATB utility-weighted total-energy level for each PHEV range and its NRCan charge-sustaining historical index. Preserve configured analogue fallbacks when a range lacks observations; do not drop that range or renormalize its sales weight. #to-review |
| Existing LDV BEVs and PHEVs | Registered NRCan range histories begin at different years | Car BEV150/200/300/400 observations begin in 2012/2013/2013/2019; PHEV35/50 begin in 2012/2013. Light-truck BEV200/300/400 begin in 2016/2016/2020; PHEV35/50 begin in 2015/2020. Earlier required years use the configured historical analogues. Light-truck BEV150 has no rating observations and zero OMEGA weight. These are source-history gaps, not unfilled final parameter rows. #to-review |
| Cars and Light Trucks | Some powertrains lack continuous historical ratings | Retain the latest own observation and extend it with the approved analogue's historical index. Where no own baseline exists, use the analogue's level scaled by the ATB powertrain relationship. #to-review |
| Cars and Light Trucks | NLR ATB light-duty hybrid diesel electric powertrains are neglected | LDV hybrid electric powertrains that utilize diesel are neglected; it is assumed that gasoline-powered HEVs will remain the dominant variant for the light-duty sector.  |
| Cars and Light Trucks | US ATB scenarios provide projected technology improvements, not Canadian fleet observations | Apply ATB relative improvements to Canadian rating-based baselines; do not substitute ATB absolute levels except for PHEVs or missing-baseline analogues. #to-review |
| LDV PHEVs | NRCan combined ratings are not used to establish the annual fuel/electricity mixture | Use ATB utility-weighted consumption levels; backcast with NRCan charge-sustaining consumption where available, otherwise the approved analogue's index. #to-review |
| PHEVs | ATB fleet utility factors describe distance in charge-depleting operation | Represent charge-depleting travel by its electricity consumption and charge-sustaining travel by its liquid-fuel consumption. Use GREET gasoline/diesel HHV-equivalent energy for the liquid component and unchanged electrical energy to derive input shares. #to-review |
| PHEVs | Fuel/electricity shares differ across ATB classes and regional truck compositions | For ordinary LDV35/LDV50 blends use pooled MTO Report A class stock counts; for MDV and HDV blends use the selected regional GVWR/vocation and haul weights. Sum weighted electrical and total consumption before taking their ratio, then average future interval-end fractions equally. Hold each regional blend constant across periods; buses retain the HDV blend. Existing LDV representatives have separate category-specific blends with their class and OMEGA range weights. #to-review |
| Medium Trucks | Ontario MTO weight-class counts and national Wards sales shares do not distinguish all ATB vocations | Use MTO Report 4 weights in ON, including Class 2/2b, and 2020 Wards Class 3–7 weights elsewhere, excluding Class 2/2b. Hold each regional mix across vintages and powertrains; weight non-school, non-transit, non-refuse vocations equally within class, consistent with costs and utilization. #to-review |
| Medium/Heavy Trucks and Buses | Selected ATB evidence lacks gasoline and CNG archetypes | Use the diesel incumbent as their efficiency proxy; intercity CNG instead uses the REGEN relative relationship. #to-review |
| Medium and Heavy Trucks | Historical CEUD fuel consumption describes the fleet rather than individual model years | Use its gasoline/diesel trajectory as a vintage backcasting index from the ATB baseline, with higher consumption implying lower efficiency. #to-review |
| Medium and Heavy Trucks | ANL Autonomie, as incorporated in NLR ATB — PHEV utility-weighted combined efficiency | Islam et al. (2023) assume a utility factor of 80%. |
| Heavy Trucks | StatCan shipment weight and distance proxy Class 8 and regional/long-haul vehicle activity | Retain shipment groups with average load plus a 13,000-kg curb weight of at least 14,970 kg. Classify average shipment distances up to 350 miles as regional DayCab and longer distances as Sleeper. Weight by each province's origin-or-destination tonne-km, counting within-province shipments once, consistently with costs and utilization. #to-review |
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
| Road Vehicles | NLR ATB vehicle prices include a retail-price-equivalent markup | Treat the selected purchase prices as retail evidence and divide by the reviewed 1.5 factor to estimate manufacturing cost. Do not apply that factor to other capital-cost sources. #to-review |
| Transport Technologies | Projected cost trajectories and historical evidence have different temporal coverage | Prospective future costs use end-of-period conditions: period 2025 reads 2030 values. Historical year-varying cost evidence uses the bin's last observed year, retaining earliest-available price proxies and manual baseline conventions. Legacy preserves the prior adapter timing. Dollar-year conversion remains independent. #to-review |
| Medium Trucks and Buses | Current normalized NLR prices lack gasoline/CNG MHDV archetypes; the legacy workbook used separate Autonomie SI/CNG series | Use the NLR diesel purchase price for gasoline/CNG medium trucks and school/transit buses. #to-review |
| Intercity buses | REGEN Chart 3 has ICEV, CNGV, BEV, and HFCV purchase prices but no HEV | Use ICEV for gasoline/diesel and apply the NLR transit-bus HEV/diesel ratio to REGEN ICEV for intercity HEV. Preserve the REGEN purchase-price basis without the NLR 1.5 adjustment. #to-review |
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
| MHDV chargers | No MHD BEV stock row exists for NB or PEI in the prepared vehicle capacity | Record zero required MHD ports and omit zero-capacity existing charger rows and dependent historical costs and efficiencies in those regions. #to-review |
| Existing and new chargers | Reviewed costs begin in 2025, while existing capacity is calibrated to 2023 stocks | Use the earliest reviewed 2025 cost rates for existing chargers' annual fixed costs, with vintage 2020 in prospective mode or 2023 in the prior legacy grid. Only new chargers receive investment rows. Future costs read vintage interval-end years in prospective mode and vintage labels in legacy mode. Apply reviewed port-count shares to per-GW costs and the OEO 1% fixed-cost ratio. #to-review |
| Charger utilization | The pinned annual capacity factor is indexed by vintage rather than calendar period | Insert vintage-based upper bounds: LDV historical chargers retain the earliest reviewed 2025 factor of 0.15; new LDV cohorts use 0.15 for a 2025 input year and 0.20 thereafter. MHDV factors are 0.30. Prospective new vintage 2025 reads the 2030 input year; legacy vintage 2025 reads 2025. A factor stays with its cohort throughout its lifetime. #to-review |

## `emission_embodied`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| New road vehicles | GREET vehicle-cycle results are lifetime totals per manufactured vehicle | Apply only to new technologies ending in `_N`; existing vehicles inherit no manufacturing emissions. Investment costs likewise apply only to new technologies. Use individual gas totals with no lifetime, mileage or annual-rate division. CO2 totals divided by 1e6 yield k tonnes/k vehicles; CH4 and N2O totals divided by 1e3 yield tonnes/k vehicles. Exclude the combined CO2 measure. #to-review |
| CH4 and N2O | Small embodied-emission coefficients widen the numerical magnitude range of the Temoa model | Express these commodities in tonnes (`t`) while CO2 remains in k tonnes (`kt`), with vehicle capacity still in thousands. CH4/N2O coefficients are therefore 1000 times their former numerical values for the same physical mass. This is the selected scaling convention to support matrix conditioning; solver stability has not been evaluated. Future operating-emission factors, limits and CO2e conversions must use the same commodity units. #to-review |
| Cars and trucks | GREET vehicle-cycle scope differs from fuel-cycle and operating emissions | Include material production/processing, vehicle manufacturing/assembly and end-of-life decommissioning without recycling credits. Transport of raw and processed materials is neglected, following the vehicle-cycle scope in the flowchart. #to-review |
| Cars and trucks | Only the current simulation target has been generated and validated | Hold the 2025 simulation's vehicle-cycle factors constant through 2050. Its imported LDV vehicle-cohort year is 2020, retained separately. GREET simulation targets through 2050 remain available when configured and regenerated; no additional solved-year evidence is assumed. #to-review |
| Cars and light trucks | GREET PHEV/BEV battery ranges and vehicle classes differ | Use the actual ATB technology ranges: PHEV 35/50 miles and BEV 150/200/300/400 miles. Map cars directly; retain the established pickup and SUV weights for light trucks and renormalize them proportionately over those groups. Gasoline, diesel and CNG use the ICEV vehicle-cycle block. #to-review |
| Medium and heavy trucks | GREET offers Class 6 and Class 8 Day/Sleeper vehicle-cycle models | Class 6 represents medium trucks while preserving scenario-selected GVWR weights. Aggregate heavy trucks with existing regional DayCab/Sleeper haul weights. HEV/PHEV share the HEV block and FCEV/FCHEV share the FCV block; MHDV factors have no material scenario and do not depend on LDV selectors. #to-review |
| LDV lightweight BEVs | The registered GREET2 glider switch selects ranges inconsistent with its adjacent labels | For AER 150/200/300/400, the native switch selects EV400/EV150/EV200/EV300 gliders. W4/Y4/AA4/AC4 contain identical inputs within each of Car, SUV and PUT in the registered release, so this mismatch has no numerical effect and lightweight factors are accepted. Preserve the native formula and automated range selectors. Recheck saved inputs on source refresh and require correction of the downstream selector if range assumptions diverge. #to-review |
| GREET vehicle-cycle results | Excel retains a global pending-calculation state after full calculations | Accept the scoped gas totals only after consecutive full calculations converge, control echoes match, range changes affect results, and reordered runs reproduce values independently of other selectors. Two independent Excel sessions reproduced identical gas evidence; this does not establish convergence of every workbook output. #to-review |

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
