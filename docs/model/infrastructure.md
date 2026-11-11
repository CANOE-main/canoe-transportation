# What infrastructure supports the vehicles?

Charging capacity is inferred from a mixture of observed public ports and
assumptions about private provision. A port count is neither rated power nor annual
energy throughput. Keeping these quantities separate explains many apparent cost
differences with legacy infrastructure estimates.

## Ports become electrical capacity

[*Transport Canada EV Dashboard*](https://tc.canada.ca/en/road-transportation/innovative-technologies/electric-vehicles/canada-electric-vehicle-dashboard)
provides public-port evidence. BEV physical stock and configured vehicles-per-port
ratios determine required ports. Private LDV ports are the required total minus
public ports; reviewed type shares distribute them. PHEVs do not enter this
stock-to-port calibration. Public-port observation dates can differ from the
vehicle-stock reference date and remain explicit in the audit.

For port type $\boldsymbol{j}$, count $\boldsymbol{n_j}$ and rated power $\boldsymbol{p_j}$ in kW,

$$K=10^{-6}\sum_j n_jp_j\quad[\mathrm{GW}].$$

!!! note "Assumption: infer private and regional port mixes"
    National public type shares proxy provincial mixes. BCT public counts combine BC
    and the territories. MHDV provision uses its own vehicle-to-port ratio and power
    mix. None of these inferences establishes observed private infrastructure coverage.

## Costs use total expenditure divided by rated power

The production estimator matches per-port capital cost $\boldsymbol{q_{jy}}$, converted
to the scenario's CAD dollar year, and rated power
$\boldsymbol{p_{jy}}$ at source year $\boldsymbol{y}$. With port-count shares $\boldsymbol{w_j}$,

$$P_y^{GW}=\frac{\sum_jw_jq_{jy}}{\sum_jw_jp_{jy}}.$$

!!! tip "Modeler note: rated-power cost is a weighted ratio"
    If $\boldsymbol{q}$ is CAD/port and $\boldsymbol{p}$ kW/port, the ratio is CAD/kW, numerically equal to
    million CAD/GW. This is not the arithmetic mean of separate per-GW rates using
    port-count shares. Each port cost is harmonized before expenditure is summed,
    so source currencies and dollar years are never averaged together. Fixed O&M
    uses the reviewed 1% capital-cost ratio while
    the infrastructure remains alive. Investment rows apply only to new chargers.

Power shares are needed **only when averaging component costs already normalized
per GW**. They are derived from the observed or reviewed port-count mix, rather
than supplied as another independent assumption:

$$\alpha_{jy}=\frac{w_jp_{jy}}{\sum_kw_kp_{ky}},\qquad
P_y^{GW}=\sum_j\alpha_{jy}\frac{q_{jy}}{p_{jy}}.$$

The production calculation uses total expenditure divided by total rated power
directly; its audit also checks this equivalent power-share sum. Applying
port-count shares to the individual per-GW rates would give a different, incorrect
denominator. For example, a 2 MW port contributes forty times as much rated
capacity as a 50 kW port, so equal port counts cannot receive equal weights in a
per-GW average.

The [*Dunsky* infrastructure study](https://natural-resources.canada.ca/energy-efficiency/transportation-energy-efficiency/resource-library/electric-vehicle-charging-infrastructure-canada)
and reviewed supplemental inputs supply port cost/power assumptions. Existing
and future rates retain their explicit source-year selection, end holds and mixes.
The [canonical infrastructure table](../assumptions.md#charging-and-hydrogen-refueling-infrastructure)
owns the detailed values.

### Verified production example

For the Ontario `validation_baseline` scenario, **model vintage 2025 selects the
2030 source-year cost and power assumptions**, following its prospective
interval-end timing. Source year, model vintage and currency year are separate
quantities. The registered CER 2026 Current Measures series converts 2022 CAD
using the deflator ratio $104.3/121.3$; L1's 2021 USD cost first uses 1.25 CAD/USD
and then $104.3/112.5$. The resulting components are:

| Charger group / type | Port-count share | Source cost per port | Rated kW per port | CAD2020 per port | Derived power share | Million CAD2020/GW |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| LDV L1 | 12% | 550 USD2021 | 1.5 | 637.39 | 1.6933% | 424.93 |
| LDV L2 | 87% | 10,000 CAD2022 | 10 | 8,598.52 | 81.8438% | 859.85 |
| LDV DCFC | 1% | 240,000 CAD2022 | 175 | 206,364.39 | 16.4628% | 1,179.23 |
| MHDV 50 kW | 82% | 74,604 CAD2022 | 50 | 64,148.37 | 29.9270% | 1,282.97 |
| MHDV 350 kW | 16% | 225,352 CAD2022 | 350 | 193,769.28 | 40.8759% | 553.63 |
| MHDV 2 MW | 2% | 629,631 CAD2022 | 2,000 | 541,389.23 | 29.1971% | 270.69 |

Count shares and power shares each sum to one **within their charger group**
before display rounding. Although 2 MW ports are only 2% of MHDV ports, they
supply 29.20% of the rated capacity represented by that mix. Their lower cost
per unit power therefore has a substantial effect on the aggregate:

| Production mix | Mean CAD2020 per port | Mean rated kW per port | Investment: million CAD2020/GW | Annual fixed cost: million CAD2020/GW/year |
| --- | ---: | ---: | ---: | ---: |
| LDV | 9,620.84 | 10.63 | 905.064865 | 9.050649 |
| MHDV | 94,432.53 | 137.00 | 689.288556 | 6.892886 |

MHDV is about **9.82 times more expensive per representative port**, but provides
**12.89 times the rated power**. It is therefore about **23.84% cheaper per GW**
under this reviewed equipment mix. This is a consequence of the cost-to-power
ratio, not a claim that installing an individual truck charger costs less than
installing an individual car charger. Annual utilization does not enter the
per-GW investment calculation; it enters only when converting rated capacity to
annual energy throughput.

!!! success "Implementation check: registered inputs reconcile with final rows"
    The native manual values, CER exchange/deflator conversion, component
    expenditure/power ratio and equivalent power-share sum reproduce the final
    `cost_invest` rows for `T_LDV_CHRG_N` and `T_MHDV_CHRG_N` at vintage 2025 in the
    recorded validation-baseline SQLite. Its fixed costs are exactly 1% of those
    investment rates and stop at the fifteen-year lifetime boundary. These checks
    validate implementation against the registered assumptions; they do not
    independently validate market prices, installation coverage or the chosen mix.

Existing chargers use first-model-year cost/power assumptions for their fixed-cost
proxy and receive no investment row. Later new vintages retain the selected
source periods; the reviewed terminal assumptions are held through 2050, rather
than presented as new observations in those later years. The
[charger-cost comparison diagnostic](../insights/transportation_validation_diagnosis.py)
retains native units, source-year selectors and conditional legacy comparisons.

## Utilization and energy boundaries

!!! note "Accounting boundary: vehicle consumption includes charging losses"
    Current reviewed utilization is 0.20 for existing/new LDV and MHDV chargers.
    With electrical capacity $\boldsymbol{K}$ in GW, 31.536 PJ/(GW·year) converts continuous power
    to annual energy: $E=31.536Ku$. The emitted charger transfer efficiency is 1.0.
    This internal accounting choice avoids counting charging losses again where
    vehicle consumption already includes them.

!!! warning "Coverage limit: motorcycle and intercity-bus charging losses"
    This boundary has limits: motorcycle consumption lacks charging losses, and the
    intercity-bus source does not establish its charging-loss boundary. A shared unity
    transfer does not resolve those gaps. PHEV input energy is counted once, not once
    for each component plus again for a blend.

!!! warning "Comparison basis: annual throughput and rated power"
    Legacy charger costs may be expressed per PJ/year rather than per GW. Conversion
    requires an explicit utilization and throughput basis. The comparison notebook
    keeps that conditional normalization separate from source-price differences and
    does not interpret missing variable-cost rows as zero costs.

![Charger investment costs on a common basis](../assets/figures/comparison-charging.svg)

*Figure comparison-charging. Backend and supplied legacy charger-cost CSVs are
shown in million CAD2020/GW after converting the legacy annual-throughput rate at
the backend utilization of 0.20. Port mixtures, source jurisdictions and equipment
boundaries still differ. The legacy variant's embedded utilization/loss assumptions
are undocumented, so this is a conditional normalization, not proof of cost parity.*

!!! info "Diagnostic finding: conditional charger-cost differences"
    For matched source years 2030–2050, the recorded backend LDV rates are 113.0–172.9%
    above the normalized legacy rates; MHDV rates are 33.5–37.6% below them. The legacy
    2025 rate has no matching backend cost-year bridge and is left unmatched. These
    gaps combine source and equipment-mix differences; the comparison does not isolate
    a causal contribution from each assumption.

## Hydrogen refueling uses a different capacity definition

Hydrogen stations use effective annual throughput in PJ/year, with implicit C2A
of one and no additional utilization factor. Their independent investment and
fixed costs originate in reviewed [*OEO* inputs](https://github.com/TemoaProject/oeo/tree/master/database_documentation),
converted from 2018 USD. Distribution/refueling efficiency is 0.995; station life
is ten years. No historical station stock is invented. Charger life is fifteen
years; both fixed-cost schedules stop at their lifetime boundary.

!!! warning "Modeling limit: capacity is not a spatial network assessment"
    These parameters describe supporting capacity and cost. They do not by themselves
    prove spatial accessibility, queueing performance or a feasible optimized network.
