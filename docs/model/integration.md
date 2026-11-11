# CANOE-main integration boundary

Standalone SQLite compilation is supported. The backend also exposes shared
prerequisite preparation, transport-contribution preparation and validated
insertion for a caller-owned database. Those interfaces preserve the same
parameterization and provenance checks without standalone publication.

!!! note "Current boundary: upstream integration remains to be verified"
    Full CANOE-main integration awaits implementation and verification of the
    upstream adapter. The upstream caller must
    own shared technologies/commodities, periods, representative slices, database
    transactions and final publication. Transport must not duplicate those structures
    or maintain an alternative parameterization.

!!! warning "Integration limit: representative time and solver feasibility"
    Final representative-period charging weights require the explicit consistency
    check described in [charging profiles](charging.md). Neither a
    successful native database build nor a prepared contribution proves integrated
    solver feasibility.