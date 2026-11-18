# How much energy does travel require?

Consumption measures energy per vehicle-km. Service efficiency measures
passenger-km or tonne-km per unit energy. The backend aggregates consumption and
the compatible load before calculating service efficiency.

## From rated configurations to fleet consumption

[*NRCan Ratings*](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64)
describe tested configurations, not actual Canadian driving. Reviewed canonical
families connect eligible configurations to [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) survivors or [*Transport Canada iZEV*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) acquisition
records. Distinct-trim medians prevent a family with many catalogue variants from
receiving many copies of its empirical count. Unsupported composition uses the
documented hierarchical catalogue fallback, with coverage retained.

### Empirical composition and matching

Matching proceeds within a vehicle class, model year and powertrain/range cohort.
Reviewed make/model descriptions connect MTO FIT_ACTIVE counts to canonical
families; eligible target-year NRCan configurations provide the consumption
ratings. MTO has no observed powertrain field, so gasoline-compatible catalogue
configurations establish the eligible gasoline match. MTO counts must not be
described as observed gasoline, HEV or electric market shares.

For PHEV and BEV cohorts, iZEV acquisition/request records supply the eligible
family composition and the configured range assignment. These counts are matched
to target-year ratings, including explicitly recorded proxy windows where
own-year composition is insufficient. Reusing a later supported composition
window for an older vintage is an assumption, not a historical registration
observation. The range labels below identify model representations; they are
not statements that every matched vehicle has exactly that rated range.

Within each supported family, distinct eligible trims first produce a median
consumption. The family's empirical count is then allocated once to that median.
Class consumption averages these family medians using normalized count weights;
outer size-class weights subsequently form the car or light-truck cohort. Where
composition cannot support a match, the documented distinct-trim, family and
cohort medians remain visible alongside the coverage denominator.

![Gasoline car composition matched to NRCan consumption](../assets/figures/ldv-eff-2-gasoline.svg)

*Figure ldv-eff-2-gasoline. The car view compares eligible catalogue families,
matched empirical descriptions, the catalogue fallback median and the production
estimate within each size class/model year. Marker sizes show count weights,
not catalogue frequency.*

![PHEV35 car composition matched to NRCan consumption](../assets/figures/ldv-eff-2-phev-35-miles.svg)

*Figure ldv-eff-2-phev-35-miles. The same matching view for the PHEV35 car
representation retains iZEV composition, range assignment and total MJ/vehicle-km
after charge-depleting/charge-sustaining accounting.*

![BEV300 car composition matched to NRCan consumption](../assets/figures/ldv-eff-2-bev-300-miles.svg)

*Figure ldv-eff-2-bev-300-miles. The BEV300 car view shows which observed
descriptions support the representative cohort and how their count-weighted
estimate differs from the catalogue median. It does not establish full-market
or surviving-fleet coverage of incentive records.*

Asterisks mark ratings after the 2023 production projection anchor. These later
catalogue and composition observations provide a diagnostic comparison. Production
uses its 2023 anchor and ATB relative changes for 2024 onward.

!!! note "Assumption: composition counts proxy travel weights"
    Family, size-class, annual-cohort and range weights have different meanings.
    Within a [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) class, using count weights also assumes a common annual distance
    across families. [*Transport Canada iZEV*](https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1) incentive eligibility and acquisition/request timing do not
    make its records a census of sales or surviving stock.

For normalized weights $w_i$, consumption $c_i$ in MJ/vehicle-km and matched
occupancy or payload $\ell_i$, the service efficiency is

$$\bar c=\sum_iw_i c_i,\qquad \bar\ell=\sum_iw_i\ell_i,\qquad \eta=\frac{\bar\ell}{\bar c}.$$

!!! example "Worked example: average consumption before inversion"
    $\boldsymbol{\eta}$ is passenger-km/MJ or tonne-km/MJ, numerically equal to billion service-km/PJ.
    For example, equally weighted consumption of 2 and 4 MJ/km gives 3 MJ/km; at two
    passengers, efficiency is $2/3$ passenger-km/MJ, not the mean of $2/2$ and $2/4$.

Each existing road vintage weights supported annual observations by raw physical
cohort stock after fuel allocation and before lifetime redistribution. Consumption
and load use the same weights, with missing-year coverage recorded before
renormalization. This step includes buses and motorcycles, which have different
[source and stock methods](vehicles.md#buses-and-motorcycles).

### How matching changes the consumption estimate

The next four figures follow one worked example, from gasoline light-truck
ratings to the consumption trajectory used for model vintages. All four steps
work in energy per vehicle-km or its fuel-volume equivalent. The model applies
occupancy or payload afterward to calculate service efficiency.

#### 1. Start from the gasoline catalogue ratings

![Native light-truck gasoline consumption](../assets/figures/ldv-eff-4-light-trucks-gasoline-l-100-km.svg)

*Figure ldv-eff-4-light-trucks-gasoline-l-100-km. Each size-class and model-year
point summarizes the NRCan catalogue in its original L/100 km. The median of
distinct-trim family medians gives the baseline for the matching comparison.*

#### 2. See what matching changes within each size class

![Composition effect on light-truck gasoline consumption](../assets/figures/ldv-eff-7-light-trucks-gasoline.svg)

*Figure ldv-eff-7-light-trucks-gasoline. Match eligible gasoline families to
Ontario MTO counts and weight the family medians within each size class. The
heatmap subtracts the catalogue baseline from that estimate on a common
MJ/vehicle-km basis. Blue shows lower consumption after matching and red shows
higher consumption. Grey means no comparison is available. White means the
estimates coincide.*

#### 3. Combine the size classes on a common energy basis

![Light-truck gasoline evidence in common energy units](../assets/figures/ldv-eff-5-light-trucks-gasoline-mj-vkm.svg)

*Figure ldv-eff-5-light-trucks-gasoline-mj-vkm. Convert the gasoline rates using
the fuel's heating value and combine the size classes with their fleet weights.
This gives one harmonized light-truck consumption estimate per supported year.
The ATB source values share the MJ/vehicle-km axis ahead of trajectory indexing.*

#### 4. Build the gasoline vintage trajectory

![Resulting light-truck gasoline vintage consumption](../assets/figures/ldv-eff-6-light-trucks-gasoline-mj-vkm.svg)

*Figure ldv-eff-6-light-trucks-gasoline-mj-vkm. Combine the annual estimates using
cohort stock weights to prepare the historical vintages. The latest observed
cohort anchors the projection, and relative changes from ATB supply the future
trajectory. Each prepared value represents the supported source years or
projection year assigned to that model vintage.*

## Trucks and the observed-to-projected bridge

!!! note "Assumption: fleet-year truck consumption proxies cohorts"
    [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) truck consumption describes a fleet year, not a measured model-year cohort.
    The backend uses this as a vintage proxy. Emerging powertrains use relative [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data)
    performance against diesel; medium-truck and heavy-truck aggregation retains the
    [weight/vocation/haul assumptions](vehicles.md#trucks-need-weight-haul-and-vocation-assumptions).
    Fuel-specific physical litres are converted with their own heating values.

For existing medium- and heavy-duty vehicles, no observed consumption series by
truck model year is available. Let $F_f(y)$ be the CEUD fleet-year rate in
L/100 km, $H_f$ the configured fuel heating value in MJ/L, and $d$ diesel. The
historical proxy assigned to cohort year $y$ is

$$c_f(y)=F_f(y)H_f/100,\qquad
c_r(y)=c_d(y)\frac{c^{ATB}_r(y_0)}{c^{ATB}_d(y_0)}.$$

The first expression applies to observed medium-truck gasoline/diesel and
heavy-truck diesel. The second estimates unobserved powertrains $r$ using the
diesel fleet history and their ATB consumption ratio at the common reference
$y_0=2023$. This holds the relative powertrain advantage fixed through the
historical proxy, while allowing the observed diesel fleet-year trend to vary.
It does not infer individual vintage performance from an aggregate fleet rate.

The latest existing cohort then supplies a stock-weighted consumption anchor
$\bar c_r$. For projected annual years,

$$c_r^{projected}(y)=\bar c_r
\frac{c^{ATB}_{q(r)}(y)}{c^{ATB}_{q(r)}(y_0)},$$

where $q(r)=r$ except that gasoline follows the configured diesel improvement
index. The anchor's payload is retained for future service efficiency. Own
powertrain stock weights are used when available; the configured diesel-stock
proxy and any incomplete cohort coverage remain in the audit. Truck weight,
vocation and haul aggregation shape the ATB ratios rather than creating observed
Canadian truck-vintage consumption.

### Projection years and range variants

On the standard five-year grid, new road vintage 2025 uses the 2030 projection
in prospective mode. The legacy opening-period route weights supported 2021–2025
annual consumption and load equally. Both use the 2023 aligned ratings/ATB
reference for LDVs, with 2024 and 2025 calculated from ATB ratios. For LDV range
variant $r$ and projected year $y$, consumption in MJ/vehicle-km is

$$c_r(y)=c_{anchor}\frac{c^{ATB}_r(y)}{c^{ATB}_{anchor}(y_0)}.$$

!!! note "Assumption: an empirical anchor with projected relative changes"
    $c_{anchor}$ is the observed class and powertrain anchor. [NLR ATB](https://atb.nlr.gov/transportation/2024/data)
    supplies the relative improvement and differences across ranges. BEV300 and
    PHEV35 provide the reference indices for their families. Source-year policies
    account for sparse coverage.

![Car BEV consumption bridge](../assets/figures/ldv-eff-6-cars-bev-mj-vkm.svg)

*Figure ldv-eff-6-cars-bev-mj-vkm. The Ontario car BEV diagnostic separates
cohort evidence from indexed range trajectories in MJ/vehicle-km. The captured
notebook build uses reference 2023 and explicit future ranges. The bridge is
constructed from an observed anchor and relative [NLR ATB](https://atb.nlr.gov/transportation/2024/data) changes; continuity is not
validation of future performance.*

![Light-truck PHEV consumption before load harmonization](../assets/figures/ldv-eff-6-light-trucks-phev-mj-vkm.svg)

*Figure ldv-eff-6-light-trucks-phev-mj-vkm. The light-truck PHEV view retains
the energy-equivalent consumption and range representations before the final
payload/occupancy division. The PHEV distance utility factor is applied to its
physical components before service efficiency is calculated.*

### Ontario medium-truck gasoline and diesel {#ldv-eff-8-medium-trucks-incumbents-mj-vkm}

![Ontario medium-truck gasoline and diesel consumption through historical and projected vintages](../assets/figures/ldv-eff-8-medium-trucks-incumbents-mj-vkm.svg)

*Figure ldv-eff-8-medium-trucks-incumbents-mj-vkm. Ontario's gasoline medium
trucks use less energy per vehicle-km in every plotted fleet year (2000–2023)
and prepared vintage (evaluated through 2050). The 2023 NRCan CEUD fleet averages
are 6.33 MJ/vehicle-km for gasoline and 7.13 for diesel. Solid lines show the
fleet-year proxies. Dashed lines connect each fuel's stock-weighted anchor
(a circle) to its new-vintage estimates using the same relative ATB diesel
improvement. Diamonds mark the consumption underlying the prepared vintages.*

![Heavy-truck diesel consumption before load harmonization](../assets/figures/ldv-eff-6-heavy-trucks-diesel-mj-vkm.svg)

*Figure ldv-eff-6-heavy-trucks-diesel-mj-vkm. Historical diesel consumption
uses CEUD fleet-year proxies; future vintages use the observed-cohort anchor and
relative ATB improvement. It is a production estimate in MJ/vehicle-km, not a
measurement of consumption by truck vintage.*

## PHEVs need component accounting

A PHEV combines charge-depleting (CD) and charge-sustaining (CS) operation. If $\boldsymbol{f}$
is the CD distance utility factor, its physical total consumption is

$$c=f(c_{CD,e}+c_{CD,f})+(1-f)c_{CS,f}.$$

!!! tip "Modeler note: distance utility and energy shares differ"
    Every component is in MJ/vehicle-km after explicit fuel/electricity conversion.
    $\boldsymbol{f}$ is a distance fraction, not the electricity fraction of energy and not annual
    vehicle utilization $\boldsymbol{u}$. Shared input-split contracts represent the resulting
    electric/liquid blend. Its total energy is counted once in reconciliation.
    The model uses a wall-energy basis for electricity. The PHEV ratings boundary
    remains subject to the qualification in [infrastructure](infrastructure.md#representation-and-accounting-limits-remain-explicit).

## Optional [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) energy calibration {#optional-ceud-energy-calibration}

!!! warning "Coverage limit: no separately reported electricity target"
    The registered [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) car/light-truck fuel tables do not separately report electricity.
    The opt-in method therefore fits a **comparable gasoline-plus-diesel boundary**
    for cars and passenger/freight light trucks, medium trucks and heavy trucks.
    Heavy-truck energy is entirely diesel: its aggregate PJ total supplies the diesel
    target directly. Within that confirmed heavy-truck total, electricity contributes
    zero; this does not identify the efficiency of future electric trucks.
    Other reported fuels and electricity without a reported target remain visible.
    Missing electricity is not a measured zero.

!!! note "Observation basis: 2023 values held at reference 2025"
    The Ontario comparison represents 2025 using the registered [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) 2023 edition
    and its explicit constant-extension policy. Stock and fuel targets retain their
    2023 observed values; the reconstructed fleet is aged to 2025. Annual utilization
    first matches the independently projected backend 2025 demand, using real GDP
    growth from the latest CEUD activity reference. Energy calibration preserves
    that projected activity. This is a reference-fleet calibration, not a new
    2025 energy observation.

!!! note "Source precision and numerical closure"
    The source reports PJ to six decimals. Independently rounded fuel components can
    differ from the rounded total by a few millionths of a PJ. The compatibility
    check uses that declared precision; fitted-energy closure uses a separate,
    tighter numerical tolerance. Signed residuals remain recorded. A substantive
    medium-truck remainder of 0.095833 PJ is retained as unresolved coverage.

For class $\boldsymbol{c}$ and technology–vintage cohort $\boldsymbol{v}$, reference physical stock $\boldsymbol{N_{cv}}$,
C2A $\boldsymbol{a_c}$, utilization $\boldsymbol{u_c}$,
uncalibrated efficiency $\boldsymbol{\eta^0_{cv}}$ and the emitted liquid-energy fraction
$\boldsymbol{\lambda_{cv}}$, predicted comparable energy in PJ is

$$E^0_c=\sum_v\frac{N_{cv}a_cu_c}{\eta^0_{cv}}\lambda_{cv},\qquad
g_c=\frac{E^{CEUD,liquid}_c}{E^0_c},\qquad \eta^1_{cv}=\frac{\eta^0_{cv}}{g_c}.$$

$\boldsymbol{N}$ is thousand surviving vehicles; $\boldsymbol{a}$ is billion service-km per thousand
vehicles per year at full utilization; $\boldsymbol{u}$ is a dimensionless annual fraction;
$\boldsymbol{\eta}$ is billion service-km/PJ; and $\boldsymbol{\lambda}$ is a dimensionless liquid-energy
fraction. All quantities use the same reference-year physical fleet and region.

!!! warning "Assumption: one effective factor transfers across technologies"
    The degree of freedom is one dimensionless consumption multiplier per supported
    region/class. It preserves fleet/activity, relative consumption trajectories and
    the reviewed PHEV split. The same multiplier is transferred to other powertrains
    and future years, including electricity and hydrogen: **that transfer is an
    explicit effective-level assumption, not a fuel-specific efficiency correction
    identified by [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)**.
    No technology-, vintage-, range- or fuel-specific correction is uniquely estimated.

!!! tip "Modeler note: supported calibration combinations"
    Historical consumption, projection anchors, future trajectories and representative
    range aggregation use the same adjustment. Unadjusted evidence remains available.
    `efficiencies.ceud_energy_calibration` is false by default; the comparison scenario
    enables it explicitly. The [*NRCan CEUD* notebook](../insights/ceud_calibration_diagnosis.py)
    shows targets, factors, excluded coverage, residuals and final-parameter changes.
    The supported combination is fixed road lifetimes and flat annual utilization;
    configuration validation rejects survival-curve or age-dependent combinations.

![NRCan CEUD calibration closure and retained boundaries](../assets/figures/ceud-cal-1.svg)

*Figure ceud-cal-1. Unadjusted and calibrated liquid energy share the left axis
with the CEUD target. The striped target extension retains excluded propane and
natural gas; unresolved coverage remains in the diagnostic audit. Markers on the right axis
show the class consumption multipliers. Fitted closure establishes arithmetic
consistency under the same fleet and activity, not independent validation of
consumption or the cause of the original mismatch.*

<div class="keep-together" markdown="1">

For the recorded Ontario 2025 fleet, with utilization matched to the backend's
projected 2025 demand, the fitted factors are:

| Class | Unadjusted comparable energy (PJ) | [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) gasoline/diesel target (PJ) | Consumption multiplier |
| --- | ---: | ---: | ---: |
| Cars | 151.375830 | 130.469305 | 0.861890 |
| Passenger light trucks | 241.974079 | 219.787701 | 0.908311 |
| Freight light trucks | 80.816287 | 71.790711 | 0.888320 |
| Medium trucks | 64.678631 | 57.445597 | 0.888170 |
| Heavy trucks | 160.698184 | 157.009704 | 0.977047 |

</div>

!!! info "Diagnostic finding: the fitted consumption adjustment"
    Consumption falls by about 2.3–13.8% on the fitted boundary. Heavy-truck service
    efficiency increases by about 2.35%. Efficiency changes
    by the reciprocal factor, not by the same percentage. These are effective class
    adjustments under the stated fleet/activity assumptions, not measurements of
    technology-specific test-to-road bias.

![Regional light-duty base-year anchor efficiency changes](../assets/figures/ceud-cal-5-ldv.svg)

*Figure ceud-cal-5-ldv. Cars, passenger light trucks and freight light trucks use
the same powertrain/range columns and all ten model regions. Cells show the
base-year range-anchor service-efficiency change, $\eta^1/\eta^0-1=1/g-1$,
before representative aggregation. Every region uses its own fitted class factor.
The common class multiplier gives emitted representatives the same percentage
adjustment; the paired final-database audit separately verifies propagation.*

![Regional medium/heavy-duty base-year anchor efficiency changes](../assets/figures/ceud-cal-5-mhdv.svg)

*Figure ceud-cal-5-mhdv. Medium and heavy trucks retain the same regional layout
and shared powertrain columns. The heavy-truck adjustment fits the confirmed
diesel aggregate. Tooltips retain before/after range-anchor efficiencies and their
units. Blank cells indicate unrepresented class/powertrain combinations rather
than a zero adjustment. Historical and future relative trajectories are preserved.*

Region codes preserve the backend's source boundaries: **BCT** combines British
Columbia with the three territories; **NLLAB** and **PEI** denote Newfoundland and
Labrador and Prince Edward Island. No BC-only calibration is inferred from BCT.

!!! note "Confirmed boundary: heavy-truck energy is diesel"
    The reviewed CEUD heavy-truck energy boundary is entirely diesel.
    Table 36's `Heavy Truck Energy Use1 (PJ)` therefore supplies the complete diesel
    energy target. It is selected once, rather than added to a separate diesel
    component. Table 37's diesel L/100 km series continues to supply the historical
    consumption proxy; the two quantities serve different stages of the calculation.
    The same class-factor method now calibrates heavy-truck consumption while
    preserving the projected activity, physical stock and relative trajectories.

!!! info "Recorded Ontario paired check"
    In the comparison accompanying these figures, 225 of 574 efficiency rows
    change by the specified class factors, including 33 heavy-truck rows. The
    maximum relative factor error is $4.44\times10^{-16}$ and liquid energy closes
    within $2.84\times10^{-14}$ PJ. Activity, PHEV input splits and other numerical
    parameters agree at the recorded tolerances. The 31 efficiency-input
    fingerprints match. The audit verifies final rows. Separate baseline
    preaggregation exports were unavailable for a byte-level comparison.
    See the [comparison commands](../publication.md#optional-energy-calibration-comparisons).
