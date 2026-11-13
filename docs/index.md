# CANOE transportation: from fleet evidence to energy system model parameters

The transportation backend explicitly represents vehicle technologies that convert energy into passenger and freight transportation services with **techno-economic** and **life-cycle** parameters. These technologies are modeled alongside their supporting infrastructure, including charging and hydrogen refueling stations.

It compiles registered observations, external technology projections and reviewed assumptions into a reproducible **CANOE/Temoa** SQLite database for energy system analysis. Its purpose is to make the connection between evidence, modeling choices and numerical parameters auditable and traceable.

This documentation follows a sequence suitable for scenario analysis of transportation systems:
``vehicles → retirement → activity → energy → technology choices → infrastructure``.
Each chapter explains what is measured, what must be inferred, and what the resulting parameters mean. It is **intended for** researchers new to the backend and for aiding its assessment and maintenance.

## What is delivered

The supported **scenario builds** prepare registered and cached inputs locally, constructs the selected parameter layers, validates schema/provenance/data-quality and publishes SQLite
atomically.

They incorporate road vehicles, air, rail, and marine modes, including direct energy use from other off-road technologies for full accounting as per the NRCan Comprehensive Energy Use Database ([*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm)). Scenario YAML controls regions, timing, survival, technology trajectories, range representation, and light-duty vehicle (LDV) charging behavior. Interactive notebooks in ``docs/insights/`` analyze captured evidence; the figures in this report are static selections from those notebooks.

## Aspects that matter for interpretation

- **The fleet is reconstructed.** Annual sales and US-transferred survival schedules
  establish cohort sizes; calibration factors rescale cohorts to match *NRCan CEUD* reference year stock. Stock agreement therefore tests calibration closure, not the accuracy of each cohort.
- **Categorical vehicle detail affects multiple parameters.** Reviewed vehicle make/model mappings, class weights, and range shares link consumption, costs and embodied emissions.
  Unmapped vehicles and transferred composition remain material limitations: 47.66%
  of the [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data) ``FIT_ACTIVE``
  coverage population is unresolved in the published snapshot.
  Composition weighting therefore represent the supported subset.
- **Activity and energy are not always in alignment.** Matching travel activity does
  not imply matching energy. Thus, energy usage from the road fleet can optionally be rescaled to fit incumbent secondary energy demand as per *NRCan CEUD* reports; it does not identify technology- and composition-specific errors.
- **Time and capacity definitions matter.** Retirement with fixed, integer lifetimes represent ``existing_capacity`` as surviving stock; retirement with survival curves use installed-equivalent vehicle sales. Observation years,
  model vintages, period endpoints and currency years are kept distinct.
- **Infrastructure costs depend on the denominator.** Charger costs are aggregated
  as expenditure divided by rated power. Hourly charging capacity factors and annual
  utilization constrain the same capacity and must remain consistent.
!!! note "Opt-in energy calibration"
    See [*NRCan CEUD* energy calibration](model/energy.md#optional-ceud-energy-calibration) for details; it is an explicit comparison with an uncalibrated reference. This scenario option is **not active by default** and does not establish scientific acceptance.

## Read and use the backend

Start with [installation, builds and evidence](getting_started.md), then the
[scenario guide](scenario_configuration.md). Follow the system chapters in order
for the complete argument, or use [modeling choices](model/choices.md) to locate a
particular decision. Detailed [assumptions](assumptions.md), [derivations and existing
flowcharts](etl_flowcharts.md), and [source inventory](source_inventory.md) remain
the reference layer.

**Terms used throughout:** LDVs are light-duty vehicles (cars and light trucks);
MHDVs are medium- and heavy-duty vehicles. BEV means battery-electric vehicle,
PHEV plug-in hybrid electric vehicle, HEV hybrid electric vehicle and FCEV
fuel-cell electric vehicle. [*NRCan CEUD*](https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm) is Natural Resources Canada's Comprehensive
Energy Use Database; [*NLR ATB*](https://atb.nlr.gov/transportation/2024/data) is the Transportation Annual Technology Baseline. [*Ontario MTO*](https://data.ontario.ca/dataset/vehicle-population-data)
is Ontario's Ministry of Transportation. Service means passenger-km or tonne-km,
which accounts for both travel distance and the passengers or freight carried.

!!! note "What these checks establish"
    Configuration validity, accounting closure, provenance integrity, legacy
    comparison and solver feasibility are separate claims. A valid database or a
    calibrated total is not an optimized transport result. The [integration
    statement](model/integration.md) identifies the current CANOE-main boundary.

The report retains unresolved scientific limits where they affect use. The
charging-simulation refactor and technology-adoption constraints remain explicitly
upcoming work.
