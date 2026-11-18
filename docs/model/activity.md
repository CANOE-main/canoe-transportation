# How much travel does the fleet provide?

Vehicle stock becomes a service only when vehicles travel and carry passengers or
freight. The backend keeps these quantities separate so a change in fleet size,
utilization or load does not silently become a change in physical efficiency.

## Stock, distance and service

For a region/class/year, let $\boldsymbol{N}$ be thousand vehicles, $\boldsymbol{d}$ annual km per vehicle,
and $\boldsymbol{\ell}$ passengers per vehicle or tonnes per vehicle. Annual service $\boldsymbol{Q}$ is
in billion passenger-km or billion tonne-km:

$$Q=10^{-6}N d\ell,\qquad \ell=\frac{10^6Q}{Nd}.$$

!!! note "Assumption: a class-average occupancy or payload"
    The factor $10^{-6}$ converts thousand vehicle-km to billion vehicle-km. [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)
    activity, distance and stock establish a class-average occupancy or payload.
    This is not an observed load for every make/model, powertrain or trip.

The model expresses the same service using a reviewed capacity-to-activity
constant $\boldsymbol{a}$ (billion service-km per thousand vehicles per year) and dimensionless
annual utilization $\boldsymbol{u}$:

$$Q=\sum_v N_v a u,\qquad u=\frac{D^{backend}_b}{\sum_vN_v a}.$$

Each road class uses its own regional service target $D^{backend}_b$ at
`base_year` $b$. This comes from the selected CEUD observation when $b=t_0$ and
from the demand projection below when $b>t_0$. The same flat factor applies to
existing and new vintages. These calculations cover cars, trucks, buses and
motorcycles.

!!! tip "Modeler note: activity uses physical survivors"
    Use retained physical survivors in the denominator. Curve-mode car and truck
    SQLite capacity describes installed-equivalent acquisitions, a different
    quantity. Validation rejects nonpositive denominators and utilization outside
    $[0,1]$. The reviewed C2A table provides the value directly.

!!! warning "Calibration closure: what activity agreement establishes"
    The fitted factors reproduce the reference-year service target under the
    assumed stock and C2A. Historical utilization ratios and their mean remain
    audit evidence. Independent validation of load, mileage and future capacity
    requires other observations.

## Age-dependent mileage evidence

The optional research route uses NLR ATB mileage profiles for cars and trucks.
It holds each native class's last mileage beyond its source-supported age, then
aggregates with the shared class and vocation weights. Normalizing to a peak of
one preserves the relative age pattern. Multiplying by the calibrated regional
class factor sets the level. Buses and motorcycles retain flat factors.

The audit covers each operating period through fixed retirement, averaging only
operating years in a partial final period. With survival curves, coverage extends
through the model horizon and stops at each process's retirement endpoint.

!!! warning "The pinned schema supports flat road utilization"
    Age-dependent factors require both operating period and vintage. The pinned
    v4 annual-factor table lacks that joint index, so standalone compilation and
    caller-owned preparation reject `vkt_schedules: true`. Use the separate
    [research route](../scenario_configuration.md#age-dependent-road-mileage) for VKT evidence.

## Observed activity is not future demand

Let $\boldsymbol{t_0}$ be the selected [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) edition's actual observation year, and $\boldsymbol{G_t}$ [*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/)
real GDP. Future service demand follows

$$D_{rt}=Q_{r,t_0}\frac{G_t}{G_{t_0}}.$$

!!! note "Assumption: national growth transferred to regional demand"
    The GDP ratio is dimensionless; $\boldsymbol{D}$ retains the service units of $\boldsymbol{Q}$. National GDP
    growth is transferred to provinces. Holding or extrapolating [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) historical
    inputs through a later reference year does not move the demand anchor to an
    invented observation. Prospective periods evaluate the closing year and legacy
    periods use the opening year. Both use the same observed-year anchor.

!!! note "Modeler choice: allocate a common passenger-demand total"
    The optional extrapolated car-demand selection uses a historical non-pandemic
    growth estimator for cars. Passenger light trucks receive the remainder of the
    unchanged combined GDP-indexed passenger demand; negative residuals fail. This
    changes the service split, not the combined target.

[*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)
and [*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/)
are the empirical and projection sources. [Detailed demand/utilization
assumptions](../assumptions.md#demand) retain the exceptions. Non-road service
inference is explained [separately](nonroad.md).
