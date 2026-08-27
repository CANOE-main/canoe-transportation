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

| Vehicle class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | StatCan vehicle sales by fuel type and size class — data begin in 2017 | The age distribution of NRCan CEUD fleet stock is estimated using vehicle registration data from the Ontario Ministry of Transportation, Quebec SAAQ, or the Insurance Corporation of British Columbia. |
| Cars and Light Trucks | StatCan pickup-truck weight classes — the NRCan light-truck weight bin differs | The NRCan CEUD light-truck class includes vehicles up to 8,500 lb, whereas StatCan's pickup-truck definition extends to 14,000 lb. The resulting difference is assumed to be negligible for fuel-type disaggregation. |
| Cars and Light Trucks | No direct data on existing charging-infrastructure capacity — `#todo` | `#todo` |
| Medium Trucks | StatCan vehicle registrations — fuel types other than gasoline and diesel have negligible counts for medium trucks | The existing medium-truck fleet is assumed to consist only of gasoline and diesel vehicles. |
| Heavy Trucks | NRCan CEUD heavy-truck fuel consumption — only diesel consumption is reported | The existing heavy-truck fleet is assumed to consist only of diesel internal-combustion-engine vehicles. |
| Buses | NRCan CEUD bus fuel consumption | Bus fuel-type shares are derived from energy consumption by fuel source, including gasoline, diesel, CNG, and electricity. |
| Buses | NRCan CEUD electricity consumption by transit buses | Existing and future transit-bus stock that uses electricity is assumed to be battery-electric and is characterized using Autonomie BEV powertrain parameters. |
| Air Transportation | NRCan CEUD aviation energy consumption — aviation gasoline accounts for less than 1% of use | All existing aviation capacity is assumed to use jet fuel. |
| Passenger Rail | NRCan CEUD passenger-rail energy consumption — electricity use is not reported | Electric-powered rail lines exist in Canada; however, NRCan reports only diesel use for passenger rail. |
| Air, Rail, and Marine | NRCan CEUD — no explicit fleet stocks | Capacity is represented in demand units (bn-pkm or bn-tkm). Fleet age distributions are estimated using an activity-based fleet-turnover approximation with an average annual retirement rate. |
| Air, Rail, and Marine | NRCan CEUD — no provincial activity data | Capacity is represented at the national level because provincial stocks cannot be traced from the source. |

## `demand`

| Vehicle class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | CER Canada's Energy Future GDP projections — do not capture the market shift from cars to light trucks | Demand growth is assumed to be uniform across vehicle classes. |

## `limit_annual_capacity_factor`

| Vehicle class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars, Light Trucks, Medium Trucks, Heavy Trucks, and Buses | NRCan CEUD — vehicle-activity-to-stock ratios have declined over recent decades | Annual utilization is held constant in future periods, excluding possible utilization changes from ride-sharing or autonomous vehicles. |
| Passenger and Freight Rail | NRCan CEUD passenger-rail activity — unclear whether the reported activity excludes intercity rail or includes commuter and transit rail | The reported activity is assumed to encompass all forms of passenger rail. Electric-powered rail cannot be added because the model lacks the infrastructure representation needed to accommodate new capacity. |
| Air, Rail, and Marine | NRCan CEUD — no explicit fleet stocks | Utilization is not represented; UF = 1. |

## `efficiency`

| Vehicle class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Wards Intelligence vehicle-sales data — proprietary and difficult to update | Wards Intelligence vehicle-sales data are used to aggregate NRCan Fuel Consumption Guide efficiencies by make and model. |
| Cars and Light Trucks | EPA OMEGA and NHTSA CAFE vehicle-sales data — public and updated, but specific to the US market | These datasets are treated as an alternative source for aggregating vehicle size, weight, and powertrain classes. |
| Cars and Light Trucks | NLR ATB — utility-weighted combined PHEV fuel and electricity consumption is unavailable because city and highway energy consumption are collapsed | Although Autonomie TEA results (Islam et al. 2023) provide exact utility-weighted fuel and electricity consumption, the simplified NLR approach is followed: the combined fleet-level utility factor is used to estimate annual-average fuel and electricity consumption (i.e., fleet-utility-weighted values). |
| Cars and Light Trucks | NLR ATB — derived annual-average PHEV fuel and electricity consumption distribution | The derived annual-average fuel and electricity consumption distribution is assumed to remain constant throughout the model horizon because `limit_tech_input_split` is indexed by `period`, not technology `vintage`. |
| Medium Trucks (Existing) | NRCan CEUD — the diesel-to-gasoline efficiency ratio is inconsistent with future projections | Existing gasoline efficiencies are estimated using multipliers from Autonomie TEA. |
| Medium and Heavy Trucks | Ontario Ministry of Transportation commercial-vehicle weight-class registrations — public and updated, but Ontario-specific | Ontario weight-class distributions are assumed to remain constant across provinces. |
| Medium and Heavy Trucks | ANL Autonomie, as incorporated in NLR ATB — PHEV utility-weighted combined efficiency | Islam et al. (2023) assume a utility factor of 80%. |
| Heavy Trucks | StatCan Canadian Freight Analysis Framework — shipment weight and distance into and out of the target province | Heavy-truck activity is classified as regional when distance is ≤ 350 miles and as long-haul when distance is > 350 miles; these shares are used to aggregate TEA parameters from Islam et al. (2023). An average Class 8 truck curb weight of 13,000 kg is assumed to estimate gross shipment weight and exclude non-Class 8 activity. |
| Passenger Rail | EPRI US REGEN electric commuter-rail cost and efficiency parameters — available from the source but currently unused | Passenger rail is not disaggregated into long-distance and commuter services; therefore, no electric passenger-rail capacity is built. |

### LDV Class Mapping by Source

| **Vehicle Model** | **NRCan Fuel Ratings** | **Autonomie TEA** | **NRCan CEUD** |
| ----------------- | ---------------------- | ----------------- | -------------- |
| **Nissan Sentra** | Midsize                | Midsize           | Car            |
| **Nissan Versa**  | Compact                | Compact           | Car            |
| **Toyota RAV4**   | Small SUV              | Small SUV         | Light Truck    |
| **Ford F-150**    | Standard pickup truck  | Pickup truck      | Light Truck    |
| **Ford Explorer** | Standard SUV           | Midsize SUV       | Light Truck    |

## `cost_variable`

| Vehicle class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | Burnham et al. (2021) and Islam et al. (2022) — empirical model coefficients are based on US mileage profiles | The same empirical-model coefficients used in these studies are applied to estimate maintenance and repair (M&R) costs per mile by vehicle age. |

## `capacity_factor_tech` for BEV charging profiles

| Vehicle class | Source / challenge | Assumption |
| --- | --- | --- |
| Cars and Light Trucks | StatCan Table 23-10-0308-01 — registered vehicle size class shares | Shares are irrespective of powertrain type given that charging profiles are also meant to represent future vehicle compositions, not just present-day fleets. |
| Cars and Light Trucks | StatCan 2021 Census — occupation demographics by province | Occupation shares between workers, students, and inactive individuals aged 15+ in private households do not differentiate between drivers and non-drivers |
