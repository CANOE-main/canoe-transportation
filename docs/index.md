# CANOE transportation: from fleet evidence to model parameters

The transportation backend describes the vehicles and infrastructure that provide
Canadian passenger and freight services. It combines registered observations,
external technology projections and reviewed assumptions into a reproducible
CANOE/Temoa SQLite database. Its purpose is to make the connection between evidence,
modeling choices and numerical parameters inspectable.

This report follows the transport system: **vehicles → retirement → activity → energy
→ technology choices → infrastructure**. Each chapter explains what is measured,
what must be inferred, and what the resulting parameters mean. It is intended for
researchers new to the backend and for assessment of the transportation MVP.

## What is delivered

The supported native build prepares registered inputs offline, constructs the selected
parameter layers, validates schema/provenance/data quality and publishes SQLite
atomically. It represents road vehicles, air, rail, marine and separate off-road
equipment. Scenario YAML controls regions, timing, survival, technology trajectories,
range representation and charging behavior. Interactive notebooks inspect captured
evidence; the figures in this report are static selections from those calculations.

The optional [CEUD energy calibration](model/energy.md#optional-ceud-energy-calibration)
is an explicit comparison with an uncalibrated reference. It is not the default and
does not establish scientific acceptance. [Vehicle-cost diagnostics](model/technology.md)
expose both investment trajectories and age-dependent maintenance and repair.

## Findings that matter for interpretation

- **The fleet is reconstructed.** Annual sales and transferred survival schedules
  establish cohort shape; one class-level factor matches CEUD reference stock.
  Stock agreement therefore tests calibration closure, not the accuracy of each cohort.
- **Vehicle detail affects several parameters together.** Reviewed make/model mappings,
  class weights and range shares link consumption, costs and manufacturing emissions.
  Unmapped vehicles and transferred composition remain material limitations: 47.66%
  of the MTO FIT_ACTIVE coverage population is unresolved in the published snapshot.
  Composition figures consequently describe the supported subset.
- **Activity and energy are different constraints.** Matching travel activity does
  not imply matching energy. The optional energy adjustment fits only an explicitly
  comparable reported fuel boundary; it cannot identify technology-specific errors.
- **Time and capacity definitions matter.** Fixed retirement uses surviving stock;
  absolute survival curves use installed-equivalent acquisitions. Observation years,
  model vintages, period endpoints and currency years are kept distinct.
- **Infrastructure costs depend on the denominator.** Charger costs are aggregated
  as expenditure divided by rated power. Hourly charging ceilings and annual
  utilization constrain the same capacity and must remain consistent.

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
fuel-cell electric vehicle. CEUD is Natural Resources Canada's Comprehensive
Energy Use Database; ATB is the Transportation Annual Technology Baseline. MTO
is Ontario's Ministry of Transportation. Service means passenger-km or tonne-km,
which accounts for both travel distance and the passengers or freight carried.

!!! note "What these checks establish"
    Configuration validity, accounting closure, provenance integrity, legacy
    comparison and solver feasibility are separate claims. A valid database or a
    calibrated total is not an optimized transport result. The [integration
    statement](model/integration.md) identifies the current CANOE-main boundary.

The report retains unresolved scientific limits where they affect use. The
charging-simulation refactor and technology-adoption constraints remain explicitly
upcoming work. They are not prerequisites silently executed by the documentation.
