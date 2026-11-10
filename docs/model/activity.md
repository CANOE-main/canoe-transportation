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

$$Q=\sum_v N_v a u,\qquad u=\frac{Q^{CEUD}}{\sum_vN_v a}.$$

!!! tip "Modeler note: activity uses physical survivors"
    Cars and trucks use calibrated physical survivors, including when curve-mode
    SQLite capacity contains installed-equivalent acquisitions. Nonpositive
    denominators and utilization outside $[0,1]$ fail. The manual C2A value is used
    directly; explanatory speed/load/hours columns do not recompute it.

!!! warning "Calibration closure: what activity agreement establishes"
    This fits reference activity by construction. It does not independently validate
    stock, load, mileage or future available capacity. Buses and motorcycles retain
    the mean of the latest five eligible observed annual ratios, excluding 2020–2021.

## Age-dependent mileage: prepared, not fully represented

[*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) mileage schedules describe how travel changes with vehicle age. Shared class
and vocation weights aggregate those profiles; normalizing to a peak of one
retains the relative age pattern while [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) supplies the level.

!!! warning "Current limitation: age-dependent mileage is audit evidence"
    The current v4 annual-factor interface cannot simultaneously index operating
    period and vintage. The opt-in VKT pathway therefore prepares age-dependent audit
    tables, while emitted annual factors remain the class-level baseline. It must not
    be described as active age-dependent utilization in Temoa. This limitation matters
    when interpreting both retirement and energy comparisons.

## Observed activity is not future demand

Let $\boldsymbol{t_0}$ be the selected [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) edition's actual observation year, and $\boldsymbol{G_t}$ [*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/)
real GDP. Future service demand follows

$$D_{rt}=Q_{r,t_0}\frac{G_t}{G_{t_0}}.$$

!!! note "Assumption: national growth transferred to regional demand"
    The GDP ratio is dimensionless; $\boldsymbol{D}$ retains the service units of $\boldsymbol{Q}$. National GDP
    growth is transferred to provinces. Holding or extrapolating [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) historical
    inputs through a later reference year does not move the demand anchor to an
    invented observation. Prospective periods use their interval-end projection year.

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
