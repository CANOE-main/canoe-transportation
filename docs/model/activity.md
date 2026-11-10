# How much travel does the fleet provide?

Vehicle stock becomes a service only when vehicles travel and carry passengers or
freight. The backend keeps these quantities separate so a change in fleet size,
utilization or load does not silently become a change in physical efficiency.

## Stock, distance and service

For a region/class/year, let $N$ be thousand vehicles, $d$ annual km per vehicle,
and $\ell$ passengers per vehicle or tonnes per vehicle. Annual service $Q$ is
in billion passenger-km or billion tonne-km:

$$Q=10^{-6}N d\ell,\qquad \ell=\frac{10^6Q}{Nd}.$$

The factor $10^{-6}$ converts thousand vehicle-km to billion vehicle-km. CEUD
activity, distance and stock establish a class-average occupancy or payload.
This is not an observed load for every make/model, powertrain or trip.

The model expresses the same service using a reviewed capacity-to-activity
constant $a$ (billion service-km per thousand vehicles per year) and dimensionless
annual utilization $u$:

$$Q=\sum_v N_v a u,\qquad u=\frac{Q^{CEUD}}{\sum_vN_v a}.$$

Cars and trucks use calibrated physical survivors, including when curve-mode
SQLite capacity contains installed-equivalent acquisitions. Nonpositive
denominators and utilization outside $[0,1]$ fail. The manual C2A value is used
directly; explanatory speed/load/hours columns do not recompute it.

This fits reference activity by construction. It does not independently validate
stock, load, mileage or future available capacity. Buses and motorcycles retain
the mean of the latest five eligible observed annual ratios, excluding 2020–2021.

## Age-dependent mileage: prepared, not fully represented

ATB mileage schedules describe how travel changes with vehicle age. Shared class
and vocation weights aggregate those profiles; normalizing to a peak of one
retains the relative age pattern while CEUD supplies the level.

The current v4 annual-factor interface cannot simultaneously index operating
period and vintage. The opt-in VKT pathway therefore prepares age-dependent audit
tables, while emitted annual factors remain the class-level baseline. It must not
be described as active age-dependent utilization in Temoa. This limitation matters
when interpreting both retirement and energy comparisons.

## Observed activity is not future demand

Let $t_0$ be the selected CEUD edition's actual observation year, and $G_t$ CER
real GDP. Future service demand follows

$$D_{rt}=Q_{r,t_0}\frac{G_t}{G_{t_0}}.$$

The GDP ratio is dimensionless; $D$ retains the service units of $Q$. National GDP
growth is transferred to provinces. Holding or extrapolating CEUD historical
inputs through a later reference year does not move the demand anchor to an
invented observation. Prospective periods use their interval-end projection year.

The optional extrapolated car-demand selection uses a historical non-pandemic
growth estimator for cars. Passenger light trucks receive the remainder of the
unchanged combined GDP-indexed passenger demand; negative residuals fail. This
changes the service split, not the combined target.

[NRCan CEUD](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)
and [CER Canada's Energy Future](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/)
are the empirical and projection sources. [Detailed demand/utilization
assumptions](../assumptions.md#demand) retain the exceptions. Non-road service
inference is explained [separately](nonroad.md).
