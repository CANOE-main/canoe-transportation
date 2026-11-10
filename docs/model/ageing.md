# How does the fleet age and retire?

Existing capacity must distinguish vehicles originally acquired from vehicles
still operating at the reference year. Temoa's lifetime representation determines
which quantity belongs in the database. This also affects the ages used in
[maintenance and repair](technology.md#maintenance-and-repair).

## Sales, survival and stock calibration

For region $r$, vehicle class $c$, acquisition year $y$ and reference year $b$,
let $A_{rcy}$ be annual acquisitions in thousand vehicles and $S_c(a)$ the fraction
surviving to age $a$ (dimensionless). Raw surviving stock is $A_{rcy}S_c(b-y)$.
The backend matches the CEUD class stock $B_{rcb}$ using one multiplier:

$$k_{rc}=\frac{B_{rcb}}{\sum_y A_{rcy}S_c(b-y)},\quad
I_{rcy}=k_{rc}A_{rcy},\quad N_{rcyb}=I_{rcy}S_c(b-y).$$

$I$ is calibrated installed-equivalent acquisitions and $N$ is physical surviving
stock, both in thousand vehicles. Raw sales are retained. One observed total
constrains the level, not the vintage-by-vintage distribution. Sales start in 2000;
the multiplier absorbs missing older acquisitions and other source differences
without identifying their causes. Annual fuel-share allocation can change the
aggregate powertrain mix when the survivor age distribution changes.

For example, raw survivors totaling 80 thousand and a CEUD target of 100 thousand
give $k=1.25$. A raw ten-thousand-vehicle surviving cohort becomes 12.5 thousand.
This is an illustration of the arithmetic, not an Ontario finding.

## What survival evidence establishes

Supported source selections are NHTSA/NEMS and MOVES. NHTSA supplies car,
light-truck and medium-truck schedules; NEMS supplies heavy-truck schedules.
MOVES supplies car, light-truck and heavy-vehicle schedules, with the heavy schedule
transferred to medium trucks. The selected source governs both historical
reconstruction and future retirement. The default uses MOVES.

Successive MTO snapshots provide **apparent registration retention**. Migration,
status changes, recoding and incomplete mapping can change counts as well as
retirement. Consequently, similarity to a NHTSA cumulative curve is not independent
validation of a physical survival schedule. Annual rates, exposed counts and
mapping coverage must accompany the cumulative comparison in the population
notebook. The recorded car result fails the monotone-survival gate; the light-truck
result fails the mapped-exposure gate (13.12% of its eligible transition exposure,
versus 59.31% for cars). Neither passes all gates, so these results do not promote
an MTO-derived schedule into production. Transition-exposure denominators differ
from the single-snapshot fleet denominator in the 100-vehicle illustration.

Primary methods are documented by [NHTSA CAFE](https://www.nhtsa.gov/corporate-average-fuel-economy),
[EPA MOVES](https://www.epa.gov/moves) and [EIA NEMS](https://github.com/EIAgov/NEMS).
The [canonical lifetime assumptions](../assumptions.md#lifetime_tech-lifetime_process-and-lifetime_survival_curve)
identify the registered schedules and their transfer limits.

![Mapped exposure behind the retention estimate](../assets/figures/population-19.svg)

*Figure population-19. The exposed FIT_ACTIVE population is divided into supported
and unresolved mapping coverage before estimating retention. A curve based on the
mapped subset cannot establish the behavior of excluded vehicles; the source
transition years and denominators remain visible.*

![Annual apparent retirement rates and exposure](../assets/figures/population-20.svg)

*Figure population-20. Pooled annual MTO apparent retirement rates retain their
starting exposure, shown by point size. Negative rates expose increases in the
tracked registration population; they must not be silently clipped into a fitted
physical-survival curve. The NHTSA comparison appears in Figure population-21.*

![Cumulative retention and NHTSA survival](../assets/figures/population-21.svg)

*Figure population-21. Accumulating annual rates makes the age pattern easier to
compare with NHTSA, but does not remove registration/mapping effects or create
independent retirement observations. Read this comparison with Figures
population-19 and population-20 and the recorded diagnostic gates.*

## Fixed retirement versus annual curves

| Representation | Exported capacity | What Temoa does |
| --- | --- | --- |
| Fixed lifetime | Calibrated surviving stock after audited lifetime-eligible redistribution | Retires the cohort at its lifetime, with a partial final model period where applicable. |
| Survival curve | Calibrated installed-equivalent acquisitions | Multiplies capacity by absolute annual survival and averages surviving capacity within a period. |

For vintage $v$ and model period $p$ containing $L_p$ annual years, curve-mode
available capacity is

$$C_{pv}=I_v\frac{1}{L_p}\sum_{t\in p}S_v(t).$$

$I_v$ and $C_{pv}$ are thousand vehicles; $S_v(t)$ is dimensionless. The backend
exports annual nodes, not period means. Feeding surviving stock into this absolute
curve would apply historical retirement twice. See the inspected [Temoa capacity
rule](https://github.com/TemoaProject/temoa/blob/cb2f51503ddb4dc6607c73635098d34c5e2d1283/temoa/components/capacity.py#L471).

Annual acquisitions are grouped into model vintages using acquisition-weighted
survival paths. Their actual acquisition years are retained. The approved tail
extends the last native annual retirement rate until survival reaches 0.5% or
less, then appends zero; an explicit age cap can shorten it. Additional retirement
needed for Temoa's numerical limit is audited, not recalibrated away.

![Survival schedules and extrapolated tails](../assets/figures/cohort-5.svg)

*Figure cohort-5. Native schedules and their shared constant-retirement extension
show where measured/model-source support ends and the tail rule begins. The tail
controls remaining future capacity; it is an explicit modeling rule rather than
additional observed survival. In the NEMS heavy-truck case, the constant-rate tail
extends beyond a century of vehicle age. This extrapolation is a material
limitation of that alternative, not evidence of such long-lived trucks.*

![Calibration of annual cohort stocks](../assets/figures/cohort-4.svg)

*Figure cohort-4. Read-only diagnostic recalculations for reference 2025 compare
both survival-source selections across ten regions, using registered prepared
inputs and recorded configuration/source hashes. They are not ten regional
production-build comparisons. Source-dependent factors align inferred survivor totals to CEUD.
The equality is fitted. It does not establish that either source reconstructs
observed annual cohorts or predicts future stock correctly.*

The [cohort notebook](../insights/stock_cohort_diagnosis.py) separates acquisitions,
physical stock, fixed redistribution, annual curves and projected survivors. A
reference-year activity calibration is also distinct from future period-average
activity after retirement.
