# CANOE transportation

The CANOE transportation backend turns fleet, travel, energy and technology
evidence into parameters for Canadian energy-system scenarios. It builds a
scenario-specific SQLite database for CANOE and Temoa, with records linking
parameters to their sources and reviewed assumptions.

## Find your starting point

| If you need to… | Start here |
| --- | --- |
| Understand the research value, supported choices and integration status | [Executive Summary](executive_summary.md) |
| Install the backend, build a scenario and interpret its output | [Getting started](getting_started.md) |
| Choose regions, periods, technology assumptions or charging representations | [Scenario configuration](scenario_configuration.md) |
| Follow the modelling argument from fleet stock to charging and fuel use | [Representing the vehicles](model/vehicles.md), then follow the model chapters in order |
| Audit a parameter or the evidence behind it | [Assumptions](assumptions.md), [parameter derivations](etl_flowcharts.md) and [source inventory](source_inventory.md) |
| Reproduce a figure or build this site and the printable report | [Publication and evidence](publication.md) |

Road, air, rail and marine technologies provide passenger or freight services.
Other off-road activities enter the energy accounting directly. The model chapters
explain how stock, retirement and utilization determine available service, how
vehicle efficiency determines energy use, and how charging and refueling connect
transport to the rest of the energy system. Use the [diagnostic notebooks](getting_started.md#prepare-diagnostics-explicitly)
to inspect the data and assumptions behind those relationships.
