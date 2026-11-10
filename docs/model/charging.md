# When can the LDV fleet charge?

The current pathway reuses registered [*RAMP-mobility*](../../config/sources.yaml) simulation outputs. It does
not rerun a travel-behavior simulation during a scenario build. Its role is to
constrain when pooled LDV charging capacity is available while retaining an annual
energy/service balance downstream.

## What the inherited simulations represent

!!! note "Assumption: composition is embedded in the simulated shape"
    [*US NHTS*](https://nhts.ornl.gov/) and
    [*Ontario TTS*](https://dmg.utoronto.ca/transportation-tomorrow-survey/)
    selections encode different travel evidence. The registered
    Ontario BEV simulation also embeds vehicle/range composition, driver activities
    and charger assumptions. Those choices are frozen in the external output. Applying
    new [*EPA OMEGA*](https://github.com/USEPA/EPA_OMEGA_Model) range weights to that shape would double-count composition.

!!! warning "Coverage limit: calendar, travel behavior and regional transfer"
    The inherited reference uses Toronto time/DST and the 2018 calendar. The [*Ontario TTS*](https://dmg.utoronto.ca/transportation-tomorrow-survey/)
    simulation has weekday-only behavioral coverage. Occupation proportions distinguish
    workers, students and inactive people, not drivers from non-drivers. Transfer of
    the Ontario shape to other regions, future periods and shared PHEV/motorcycle
    charging is therefore an explicit proxy.

## Native normalization and utilization scaling

Current ingestion converts the registered minute-resolution power series to hourly
means and retains native energy/peak checks. Let $\boldsymbol{f_h}$ be a dimensionless normalized
hourly shape, $\boldsymbol{\bar f}$ its mean across the complete native 8,760-hour year, and $\boldsymbol{u}$
the reviewed annual charger utilization. The emitted profile is

$$f'_h=f_h\frac{u}{\bar f},\qquad
\frac1{8760}\sum_hf'_h=u.$$

!!! tip "Modeler note: hourly ceilings and annual utilization"
    The native shape is retained; the amplitude changes. Values outside the physical
    capacity-factor bounds fail validation. The hourly values are ceilings, accompanied
    by an annual utilization floor $0.98u$ when profiles are enabled. At current
    $u=0.20$, that floor is 0.196. Without a profile, annual utilization equals 0.20.
    This $\boldsymbol{u}$ is not the PHEV distance utility factor used in consumption calculations.

## Representative time can change the annual mean

An explicit hour-to-slice mapping projects the profile to model slices. A selected
set of days with final slice weights $\boldsymbol{\omega_s}$ generally has a different mean:

$$\bar f_{rep}=\frac{\sum_s\omega_sf'_s}{\sum_s\omega_s}.$$

!!! warning "Integration limit: final time weights require their own check"
    The native normalization guarantees the full-year mean, not $\bar f_{rep}=u$.
    Final CANOE-main representative-period weights therefore require a separate
    rescaling/check. That correction is not implemented by native normalization and
    must not be claimed as delivered integration. Upstream electricity and pooled
    LDV chargers retain hourly resolution; charged-electricity output and vehicle
    activity are annual. Forcing the downstream annual demand into each hour would
    create an artificial flat charging requirement.

## Planned refactor

!!! note "Upcoming methodology review"
    The future refactor should revisit the travel/fleet/charger assumptions, regional
    and weekend coverage, time reference and final representative-weight contract.
    The current backend reuses the inherited simulations. The
    [registered source and profile assumptions](../assumptions.md#capacity_factor_tech-for-ldv-charging-profiles)
    record the detailed methodology and source qualifications.