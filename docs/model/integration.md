# CANOE-main integration boundary

Standalone SQLite compilation is supported. The backend also exposes shared
prerequisite preparation, transport-contribution preparation and validated
insertion for a caller-owned database. Those interfaces preserve the same
parameterization and provenance checks without standalone publication.

An upstream-specific `canoe_adapter.py` is not present in the inspected checkout.
Full CANOE-main integration is therefore not claimed. The upstream caller must
own shared technologies/commodities, periods, representative slices, database
transactions and final publication. Transport must not duplicate those structures
or maintain an alternative parameterization.

Final representative-period charging weights require the explicit consistency
check described in [charging profiles](charging.md). The existing
[integration routing snapshot](../canoe_main_orchestrator.md) is dated context;
verify its upstream branch/interfaces before implementing an adapter. Neither a
successful native database build nor a prepared contribution proves integrated
solver feasibility.
