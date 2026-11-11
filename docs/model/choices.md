# Choose a representation for the question

Alternative representations preserve the ability to distinguish an empirical
uncertainty from a modeling simplification. They also make comparisons with older
work possible without silently changing the common baseline. A selector is useful
when its intention and affected parameter relationships are clear.

| Modeling purpose | Relevant choice | What changes; what to check |
| --- | --- | --- |
| Represent retirement detail | Fixed lifetimes or annual survival; [*EPA MOVES*](https://www.epa.gov/moves) or [*NHTSA CAFE*](https://www.nhtsa.gov/corporate-average-fuel-economy)/[*EIA NEMS*](https://github.com/EIAgov/NEMS); tail controls | Acquisition versus surviving-capacity basis, future availability and cohort composition. Check physical stock calibration and complete annual curve endpoints. |
| Use later reference years | [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) `error`, `constant` or `extrapolated`; period mode/grid | Which inputs are observations versus estimates and which years a vintage represents. Demand retains its observed-year anchor. |
| Examine efficiency uncertainty | [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) efficiency trajectory; optional [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) energy calibration | Consumption levels/relative improvements and fitted fuel totals. Cost trajectories are selected separately; energy closure is not independent validation. |
| Retain or compress range detail | No range shares, fixed new-capacity shares, representative archetype | Optimizer freedom and joint consumption/cost/embodied-mass aggregation. Confirm common utilization/lifetime and PHEV component limits. |
| Examine vehicle economics | Cost trajectory, currency/year, financing parameters | Comparable monetary values and period/source-year alignment. Do not infer technology choice from purchase price alone. |
| Represent charging time | None, [*US NHTS*](https://nhts.ornl.gov/) or [*Ontario TTS*](https://dmg.utoronto.ca/transportation-tomorrow-survey/); explicit time mapping | Hourly ceilings versus annual equality/floor, inherited behavior and final representative-weight limitation. |
| Explore activity by age | VKT schedule preparation | Current age-dependent products are diagnostic; the schema still emits the flat annual utilization representation. |
| Explore service growth or material choice | [*CER CEF*](https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/) demand scenario/car split; conventional/lightweight LDV materials | Demand allocation and new-vehicle manufacturing mass. Their scientific assumptions remain distinct from source-data quality. |

Use the [scenario guide](../scenario_configuration.md) for exact YAML fields,
supported combinations and run commands. It is the single practical selector
reference. Methods in this report explain the rationale; the source/configuration
registry and typed validators determine what can execute.

## Compare like with like

!!! tip "Modeler note: hold the comparison basis fixed"
    A comparison should identify its source edition, region, reference year, actual
    observation years, physical capacity basis, service units, fuel boundary and
    representation. Change only the intended choice. Keep the uncalibrated baseline
    when assessing calibration, and separate defect corrections from that effect.

!!! warning "Evidence limit: reference variants have distinct identities"
    The legacy fleet reconstruction and charger-cost comparison describe different
    reference variants. The original inherited-input SQLite identity is unresolved
    for part of the legacy energy reconstruction; saved outputs and active notebook
    recipes are not interchangeable evidence. Recorded comparisons retain their
    configured absolute/relative tolerances and original period/vintage labels.
    Passing a database tolerance is not evidence of scientific equivalence where
    the quantities or coverage differ.

![Legacy activity and energy differences on the recorded comparison basis](../assets/figures/comparison-activity-energy.svg)

*Figure comparison-activity-energy. Ontario at `base_year = 2025`. The activity
zero uses the backend's projected demand: latest observed CEUD activity multiplied
by CER real GDP(2025)/real GDP(observation year). Only colored model results are
shown. The upper view preserves the total-energy comparison; the lower view uses
the supported gasoline/diesel boundary and adds actual production energy
calibration. Blue squares are `v2.0 w/o efficiency calibration`; green crosses are
`v2.0 after energy calibration` and close at zero on the fitted boundary. Legacy
liquid splits remain unresolved, so legacy results appear only in the total-energy
view. Heavy trucks use the confirmed all-diesel CEUD aggregate. Legacy recipe reconstruction does
not establish which SQLite a historical execution produced.*

!!! warning "Interpretation limit: total-energy residuals depend on the boundary"
    Current activity closes to the backend's projected `base_year` demand for all
    five sales-derived road classes. The total-energy view uses
    vehicle-input energy, including electricity, against [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)'s reported class total;
    it is a **boundary-sensitive residual**, not an identified efficiency error.

| Calibration step | What changes | Reference and remaining limit |
| --- | --- | --- |
| Activity through UFs | One class UF closes `Σ(physical stock × c2a × UF)` while retaining stock and reviewed c2a. | Native CEUD observed activity indexed by real GDP to `base_year`; later service demand follows its separate trajectory. |
| Energy through efficiencies | One supported class consumption multiplier closes represented gasoline/diesel energy with activity, stock and PHEV energy splits fixed. | Reported CEUD liquid energy; electric consumption is transferred by the same factor without an observed electricity target. |
| Total-energy diagnostic | Reports all vehicle-input energy and each method's own assumptions. | It includes fuels outside the fitted liquid boundary; a remaining total-energy residual is compatible with exact liquid closure. |

Demand remains anchored to the latest CEUD observation rather than the stock
extension policy. If that observation predates `base_year`, calibrating UFs to a
held historical activity would leave a gap before the projected demand trajectory
begins. Production now uses the same reference-year projection as the demand owner.
Changing a common UF scales activity and energy together; the subsequent
efficiency fit changes energy while preserving that calibrated activity.

!!! note "Recorded legacy comparison identity"
    The comparison is Ontario reference 2025 with [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) 2023 held constant. The inherited
    recipe is `Update data_20260618_fixed lifetime (1).ipynb` (SHA-256 starts
    `ada5b14fca8d`); its active `fixed_lifetime=False` argument takes precedence over
    the filename. The explicit inherited snapshot `canoe_ON_16d.sqlite` has hash prefix
    `b4db7481fb0f`. The original named all-region input is unavailable, so historical
    execution equivalence remains unresolved. Full identities are in the figure
    manifest and captured comparison.

!!! tip "Modeler note: numerical tolerances do not resolve scope differences"
    Configured database row comparisons use absolute tolerance $10^{-9}$ in each
    parameter's native unit and relative tolerance $10^{-6}$. Those tolerances do not
    resolve different capacity definitions, missing coverage or source identities;
    the reported legacy differences are not described as a parity pass.

## Read the status correctly

!!! note "Read implementation and scientific status separately"
    **Implemented** means code and configuration support the behavior. **Reviewed
    assumption** means a documented decision supplies missing evidence. **Diagnostic
    finding** describes a specific recorded calculation. **Unresolved question** marks
    a limit that can change interpretation. **Proposed improvement** is not executable
    behavior. A review marker or successful build does not promote one status to another.
