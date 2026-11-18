# Research on technology adoption rates

An optimizer can select rapid adoption when costs and constraints permit it.
Historical diffusion can inform growth scenarios if the measurement, starting
stock and market match the question. Starting from a small stock, the same
absolute additions give a much larger percentage increase.

!!! warning "Evidence limit: stock growth, registrations and shares differ"
    The existing [growth diagnostic](../insights/growth_constraint_diagnosis.py)
    examines registered [*HATCH*](https://zenodo.org/records/10231865)/[*Extended HATCH*](https://zenodo.org/records/19579793) and [*EAFO Norway*](https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/norway/vehicles-and-fleet) histories. It compares
    stock growth, new registration rates and market shares over different windows.
    A stock growth rate measures the change in the surviving fleet. A registration
    share measures the new-vehicle mix. They need different denominators and
    imply different model constraints.

The notebook explores candidate growth formulations and deployment seeds.
This workflow is diagnostic only. Production scenario builds omit adoption
constraints from this research. Before promoting a formulation, establish which
regions, vehicle classes and maturity stages its evidence can describe. See
the [research-source inventory](../source_inventory.md#growth-research-inputs).
