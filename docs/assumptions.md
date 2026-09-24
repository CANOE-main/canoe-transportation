---
title: CANOE-Transportation assumptions
role: Review record for source limitations, data gaps, modelling challenges, and their current handling.
retrieve_when: A task changes or discovers a transportation assumption, source limitation, data gap, or modelling challenge.
read_scope: Read only the affected parameter section and relevant rows.
verify: "Check current code, config, tests, and evidence; mark Codex-added or changed review content #to-review."
---

# CANOE-Transportation Assumptions

Each parameter section is presented as a single table. Rows are grouped by vehicle class, and assumptions that apply to multiple classes remain consolidated rather than duplicated.

## `existing_capacity`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Ontario MTO is the available vehicle-population evidence | Mapped 2025 Report A age shares distribute 2023 CEUD stocks in every CEUD region. #to-review |
| Cars and Light Trucks | StatCan LDV vehicle types differ from CEUD classes | Passenger-car fuel shares apply to cars; combined pickup, multipurpose-vehicle, and van shares apply to both light-truck classes. #to-review |
| Cars and Light Trucks | CEUD and StatCan pickup weight bins differ | CEUD light trucks end at 8,500 lb; StatCan pickups extend to 14,000 lb. Treat the mismatch as negligible for fuel shares. #to-review |
| Cars and Trucks | StatCan fuel registrations start after some surviving vintages; some fuels lack existing technologies | Earlier vintages repeat the oldest available gasoline/diesel shares after renormalization. Other unsupported shares are excluded and audited. #to-review |
| Road Vehicles | Regional fuel evidence is incomplete | BC shares proxy BCT; Canada-level LDV shares proxy Alberta and Newfoundland and Labrador. CEUD NL/PE map to native NLLAB/PEI. #to-review |
| Medium Trucks, Heavy Trucks, and Motorcycles | Ontario MTO Report 5 is the available age evidence | The 2025 COMMERCIAL ages proxy both truck classes; MOTORCYCLE ages proxy motorcycles across CEUD regions. #to-review |
| Medium and Heavy Trucks | StatCan truck weight groups do not match CEUD exactly | The 10,001–33,000 lb groups proxy medium trucks and Class 8 proxies heavy trucks; 8,501–10,000 lb is uncovered. #to-review |
| Medium Trucks | Transport Canada's 2026 year-to-date medium/heavy EV share | Treat the reported share as medium-truck BEV stock in the 2023 vintage; split the remainder by StatCan gasoline/diesel proportions. #to-review |
| Road Vehicles | Fixed-lifetime vintage eligibility | Cars/light trucks use medians derived from accepted NHTSA curves; medium trucks use the NEMS Class 4–6 median; heavy trucks/motorcycles use reviewed manual lifetimes. #to-review |
| Heavy Trucks and Motorcycles | Existing incumbent powertrains | Retain diesel heavy trucks and gasoline motorcycles only; exclude and audit non-diesel heavy-truck registration shares. #to-review |
| Buses | CEUD reports bus stocks by class and annual fuel use by class, but not stock by powertrain | Split 2023 stocks using each cohort year's CEUD fuel energy times its annual bus efficiency. Map ethanol to gasoline and biodiesel to diesel; exclude propane with an audit. #to-review |
| Buses | Ontario MTO Report 5 provides the available age profile | Apply its 2025 BUS ages to all CEUD bus classes and regions; use 2000 activity shares for pre-2000 cohorts. #to-review |
| Buses | Some historical fuel shares have no cohort surviving to the first model period | Use StatCan bus lifetimes to redistribute retired cohorts within technology; move stock with no eligible technology cohort proportionally to eligible technologies in the same region and bus class, with a transfer audit. #to-review |
| Air Transportation | NRCan CEUD aviation fuel use | Use aviation turbo fuel only; exclude and audit aviation gasoline. #to-review |
| Passenger Rail | NRCan CEUD does not report electric passenger-rail energy | Build diesel passenger-rail capacity only; electric rail remains unrepresented. #to-review |
| Air, Rail, and Marine | NRCan CEUD does not report provincial fleet stocks | Divide provincial energy by same-year national intensity to estimate capacity in billion passenger-km or tonne-km. #to-review |
| Air, Rail, and Marine | Annual turnover and CIMS manual lifetimes | Each cohort loses `1/lifetime` of its effective starting capacity annually. Retire excess survivors proportionally or add capacity so each year's total matches its CEUD proxy. #to-review |
| Marine Freight | CEUD reports separate HFO/MDO energy but one marine intensity | Use the shared marine intensity and combined fleet turnover, then split each active vintage by provincial 2023 HFO/MDO energy shares. #to-review |
| Road and Off-road | Annual cohorts and configured existing periods | Assign each eligible annual cohort to the next existing period, ending at the base year. #to-review |
| Road and Off-road | Capacity must survive to the first model period | Redistribute fully retired cohorts' base-year capacity proportionally among eligible vintages of the same region and technology. Accepted road survival curves cover cars, light trucks, medium trucks, and heavy trucks. #to-review |
| LD and MHD EV Chargers (planned) | NRCan/Dunsky — unaccounted existing private charger capacity | Dunsky's 1 LD EV/port and 1.5 MHD EV/port ratios estimate private ports; NRCan type shares distribute LD ports, and Dunsky 100/350-kW shares distribute MHD ports. #to-review |
| LDEV Chargers (planned) | Transport Canada — undisclosed capacity vintage | Assign all reported capacity to the latest existing period. #to-review |

## `demand`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Air, Rail, and Marine | NRCan CEUD reports provincial energy but only national service intensity | Apply the same-year national intensity to each province's total mode energy to estimate provincial activity. Total air energy includes aviation gasoline, although existing incumbent capacity excludes it. #to-review |
| All Transport Services | CER Canada's Energy Future macro indicators provide Canada-wide real GDP only | Apply national real GDP growth to each province's own CEUD service baseline; provincial GDP differences are not represented. #to-review |

## `limit_annual_capacity_factor`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | NRCan CEUD annual vehicle activity and stock; recent activity-to-stock ratios have declined | Use the mean of the latest five eligible annual activity-to-stock ratios, excluding 2020–2021, to establish each region and road class's utilization baseline. #to-review |
| Cars and Trucks | NLR ATB age-based mileage profiles describe mileage patterns rather than Canadian utilization levels | Use each aggregated ATB profile only as a relative age trajectory, normalized to a peak of one; retain the CEUD-derived baseline as the utilization level. #to-review |
| Cars and Light Trucks | Ontario MTO vehicle-population evidence supplies the reviewed ATB class weights | Apply the all-vintage Ontario weights across CEUD regions; passenger and freight light trucks share the resulting Light Truck age pattern. #to-review |
| Medium Trucks | Wards reports national sales shares by weight class, while ATB has several mileage profiles within some classes | Use 2020 national Wards Class 3–7 shares as a proxy across regions; within each class, weight distinct non-bus freight mileage profiles equally. #to-review |
| Heavy Trucks | StatCan freight tonne-kilometres distinguish regional and long-haul activity by province | Use provincial regional/long-haul tonne-kilometre shares to combine ATB Class 8 DayCab and Sleeper mileage profiles. #to-review |
| Air, Rail, and Marine | NRCan CEUD — no explicit fleet stocks | Utilization is not represented; UF = 1. |

## `capacity_to_activity`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | Manual `capacity_to_activity` values are arbitrary activity-per-vehicle scaling factors, not measured source values | Treat each road-class factor as a model scaling assumption shared across regions and powertrains. #to-review |

## `lifetime_tech` and `lifetime_survival_curve`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | US NHTSA CAFE survival schedules proxy Canadian vehicle survival | Use reported cumulative car rates directly. Combine Vans/SUVs and Pickups for both Canadian light-truck classes using the latest reviewed Wards class shares renormalized over those two source classes. #to-review |
| Medium and Heavy Trucks | US EIA NEMS annual scrappage schedules proxy Canadian trucks | Derive cumulative survival from 100% at age zero; use Class 4–6 for medium trucks and Class 7–8 for heavy trucks without pooling the source classes. #to-review |
| Cars, Light Trucks, and Medium Trucks | Their fixed lifetimes are derived from accepted survival schedules | Use the first annual age at which cumulative survival reaches 50% or less as the fixed lifetime. #to-review |
| Cars and Light Trucks | Ontario MTO cohort changes also reflect migration, registration status, and administrative changes | Keep apparent MTO survival as comparison evidence; use accepted external survival schedules for model parameters. #to-review |
| Buses | StatCan Table 34-10-0254-01 measures publicly owned public-transit assets and has suppressed provincial cells | Apply its bus useful lives to urban transit, school, and intercity buses. Use each province's latest reported 2016–2020 value by powertrain, then Canada 2020 where none is reported, including BCT. #to-review |
| Buses | StatCan has no gasoline, plug-in hybrid, or fuel-cell bus useful-life series | Use diesel bus useful life for gasoline buses and electric bus useful life for plug-in hybrid and fuel-cell buses. #to-review |
| Other Road, Off-road, and Infrastructure Technologies | Reviewed manual lifetime evidence is at vehicle or equipment category level | Apply each reviewed category lifetime across its technologies and regions where no accepted road curve or StatCan bus lifetime supplies that technology. #to-review |

## `efficiency`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Road Vehicles | CEUD activity, annual distance, and stock describe whole vehicle classes, not individual powertrains | Derive class occupancy/payload as activity divided by distance and stock; apply it across powertrains and hold the base-year factor constant for future vintages. #to-review |
| Cars and Light Trucks | NRCan model-year ratings are not measurements of the surviving fleet's in-use consumption | Treat ratings as vintage-specific fleet efficiency proxies, without a separate aging adjustment; weight rating observations equally within each class and powertrain. #to-review |
| Cars and Light Trucks | Reviewed Ontario MTO class shares are not province-, powertrain-, or future-specific | Apply all-vintage Ontario class weights across provinces, powertrains, and vintages; passenger and freight light trucks share the same vehicle-class mix. #to-review |
| Cars and Light Trucks | Historical ratings combine SUV sizes and omit some class/powertrain combinations | Give unsplit SUVs the combined small/standard SUV weight; redistribute missing-class weights only across observed classes. #to-review |
| Cars and Light Trucks | NRCan ratings lack a complete HEV flag | Identify hybrids by model-name wording or unambiguous year/make/model matches to US FuelEconomy.gov hybrid records. Otherwise retain the reported fuel classification, which can leave unidentified HEVs among conventional vehicles. #to-review |
| Cars and Light Trucks | NRCan electric ranges do not coincide with ATB archetypes | Use fixed range bands to represent BEV and PHEV archetypes rather than requiring identical rated ranges. #to-review |
| Cars and Light Trucks | Some powertrains lack continuous historical ratings | Retain the latest own observation and extend it with the approved analogue's historical index. Where no own baseline exists, use the analogue's level scaled by the ATB powertrain relationship. #to-review |
| Cars and Light Trucks | US ATB scenarios provide projected technology improvements, not Canadian fleet observations | Apply ATB relative improvements to Canadian rating-based baselines; do not substitute ATB absolute levels except for PHEVs or missing-baseline analogues. #to-review |
| LDV PHEVs | NRCan combined ratings are not used to establish the annual fuel/electricity mixture | Use ATB utility-weighted consumption levels; backcast with NRCan charge-sustaining consumption where available, otherwise the approved analogue's index. #to-review |
| PHEVs | ATB fleet utility factors describe distance in charge-depleting operation | Represent charge-depleting travel by its electricity consumption and charge-sustaining travel by its liquid-fuel consumption. Use GREET gasoline/diesel HHV-equivalent energy for the liquid component and unchanged electrical energy to derive input shares. #to-review |
| PHEVs | Four blends represent vehicles with different fuel/electricity consumption shares | Average energy shares equally across source classes and future interval endpoints for LDV35, LDV50, MDV and HDV. Hold each blend constant across regions and periods; buses use the HDV blend. #to-review |
| Medium Trucks | Ontario MTO weight-class counts do not distinguish all ATB vocations | Apply Ontario weight-class shares across provinces and vintages; weight non-school, non-transit, non-refuse vocations equally within each class. #to-review |
| Medium/Heavy Trucks and Buses | Selected ATB evidence lacks gasoline and CNG archetypes | Use the diesel incumbent as their efficiency proxy; intercity CNG instead uses the REGEN relative relationship. #to-review |
| Medium and Heavy Trucks | Historical CEUD fuel consumption describes the fleet rather than individual model years | Use its gasoline/diesel trajectory as a vintage backcasting index from the ATB baseline, with higher consumption implying lower efficiency. #to-review |
| Medium and Heavy Trucks | ANL Autonomie, as incorporated in NLR ATB — PHEV utility-weighted combined efficiency | Islam et al. (2023) assume a utility factor of 80%. |
| Heavy Trucks | StatCan shipment weight and distance proxy regional/long-haul vehicle activity | Split at 350 miles and assume a 13,000-kg curb weight to identify Class 8 shipments. Use tonne-km haul shares, equal weights across the three regional DayCab vocations, and Sleeper trucks for long haul. #to-review |
| Buses | CEUD historical intensity is class-specific but not powertrain-specific | Apply each bus class's intensity trend across its existing powertrains; use ATB school/transit levels for future vehicles, weighting the two school-bus classes equally. #to-review |
| Intercity Buses | ATB transit buses and REGEN relative efficiencies proxy intercity technology performance | Use transit-bus efficiency levels with REGEN intercity powertrain ratios; retain the ATB transit HEV relationship where REGEN has no HEV series. #to-review |
| Motorcycles | GCAM Canada supplies technology trajectories rather than provincial motorcycle observations | Apply GCAM gasoline/BEV intensity relationships to each province's CEUD motorcycle baseline. #to-review |
| Air, Rail, and Marine | CEUD service intensities are national mode averages, not province- or powertrain-specific | Apply the national historical intensity across provinces as the incumbent baseline; use reviewed relative multipliers for alternative powertrains. #to-review |
| Off-road Modes | Reviewed EPRI US-REGEN assumptions proxy Canadian future performance | Apply the same relative technology factors and compound annual efficiency improvements across provinces; interpolate hydrogen-rail ratios between the reviewed 2035 and 2050 values. #to-review |
| Passenger and Freight Rail | REGEN natural-gas rail assumptions describe CNG, not LNG | Use the CNG-to-diesel efficiency relationship for LNG rail technologies. #to-review |
| Marine Freight | CEUD reports one marine intensity; GREET's HFO/MDO ratio describes a predefined trip | Treat CEUD intensity as the MDO baseline and apply the GREET HFO/MDO consumption ratio across historical and future vessels, regions, and vintages. #to-review |
| Aviation | No separate SPK aircraft-efficiency relationship is used | Assign SPK aircraft the same service-per-energy efficiency and improvement rate as jet-fuel aircraft. #to-review |

### LDV Class Mapping by Source

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
| Road MHDV and buses | Current normalized NLR prices lack gasoline/CNG MHDV archetypes; the legacy workbook used separate Autonomie SI/CNG series | Use the configured diesel purchase price for gasoline/CNG MDV and school/transit buses in this layer. Apply the 1.5 retail-price reversal only to NLR road purchase prices. #to-review |
| Intercity buses | REGEN Chart 3 has ICEV, CNGV, BEV, and HFCV purchase prices but no HEV | Use ICEV for gasoline/diesel and apply the NLR transit-bus HEV/diesel ratio to REGEN ICEV for intercity HEV. Preserve the REGEN purchase-price basis without the NLR 1.5 adjustment. #to-review |
| Aircraft | FAA cargo capacity is labelled tons and its aggregate utilization differs by passenger/cargo service | Interpret tons as U.S. short tons; use All Aircraft speed, capacity, load, and 365 times average daily utilization to normalize CIMS capital cost per annual service output. SPK uses jet-fuel aircraft cost. #to-review |
| Rail and marine | REGEN supplies relative 2035/2050 capital-cost factors against CIMS incumbent baselines | Keep each CIMS capital cost paired with its service output; linearly interpolate alternative factors from 2035 to 2050 and hold the 2035 factor earlier. #to-review |
| Marine freight | The reviewed manual HFO factor has no external citation | Apply its recorded one-to-one MDO capital-cost parity and retain that manual treatment in the cost audit. #to-review |
| Cost source metadata | REGEN, GCAM, BEAN, and FAA price-year labels are incomplete | Use the explicitly provisional native currency/dollar-year values in `sources.yaml` for CER conversion. Review and revise those entries when stronger source evidence is available. #to-review |

## `cost_variable`

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Burnham et al. (2021) and Islam et al. (2022) — empirical model coefficients are based on US mileage profiles | The same empirical-model coefficients used in these studies are applied to estimate maintenance and repair (M&R) costs per mile by vehicle age. |
| Cars and Light Trucks | ATB MSRP prices begin in 2023; Burnham coefficients are calibrated on a 2020-dollar MSRP basis and age coefficients end at 14 | Feed the selected retail MSRP directly to the calibrated repair equation, use its first ATB year for older existing vintages, and hold repair constant after age 14. Convert resulting source-year money to 2020 CAD with CER. #to-review |
| Medium/Heavy Trucks and Buses | BEAN provides Class 4/6 box, Class 8 haul, and Class 8 transit coefficients | Reuse road weights; proxy GVWR 2–5 with BEAN Class 4 and 6–7 with Class 6, and use Class 8 transit for bus maintenance. Scale school/intercity bus OPEX by the transit-bus cost-to-purchase relationship. #to-review |
| Road and off-road | Variable costs are service-activity costs indexed by period and vintage | Convert per-mile or per-vehicle-year evidence using CEUD occupancy/payload and annual distance; repeat age-dependent curves from each applicable vintage. Use the reviewed class-specific OEO ratio against the same CIMS baseline for existing and new rail/marine technologies. #to-review |

## `capacity_factor_tech` for BEV charging profiles

| Technology class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | StatCan Table 23-10-0308-01 — registered vehicle size class shares | Shares are irrespective of powertrain type given that charging profiles are also meant to represent future vehicle compositions, not just present-day fleets. |
| Cars and Light Trucks | StatCan 2021 Census — occupation demographics by province | Occupation shares between workers, students, and inactive individuals aged 15+ in private households do not differentiate between drivers and non-drivers |
