---
title: CANOE-main integration retrieval guide
role: Verified upstream routing context for transportation integration-sensitive work.
retrieve_when: A task affects the CANOE-main sector lifecycle, shared database, base configuration inheritance, fuel coupling, or a transportation adapter.
read_scope: Re-check the branch head, then inspect only the upstream files named for the affected seam.
verify: CANOE-main is unfinished; current upstream code outranks this snapshot.
upstream_repo: https://github.com/CANOE-main/CANOE
upstream_branch: yep/base-and-commercial
upstream_commit_reviewed: ad14685837d4989e935aa1ff20a5af881a51ad44
reviewed_on: 2026-09-04
---

# CANOE-main integration retrieval guide

## Verified snapshot

At the reviewed commit, CANOE-main creates one shared `canoe_schema` v4 SQLite database,
adds base-owned region, period, time-slice, and global metadata, then invokes each configured
sector against that database. Representative-period processing and TEMOA execution follow
sector compilation. CANOE-main does not merge independently compiled sector databases. There
is no transportation sector implementation in this branch.

The current sector interface has two methods:

```python
class CANOEModule(ABC):
    def run(self) -> CANOEModuleOutput: ...
    def get_dataset_code(self) -> str: ...
```

`CANOEModuleOutput` currently contains only `fuel_imports`. `sector_config.py` is a
discriminated Pydantic union containing only the commercial config. The commercial config
inherits selected base values such as the database path, periods, provinces, and cache config.
Its builder opens a transaction on the shared database and validates
base-owned prerequisites, but its current execution path does not yet insert commercial
sector rows. These are point-in-time implementation facts, not transportation policy.

## Transportation boundary to preserve

Transportation has two assembly clients but one transformation path:

```text
normalized evidence
    -> transport parameterization
    -> canoe_schema row contracts and provenance
    -> standalone build OR future CANOE-main adapter
```

Parameterization remains independent of SQLite lifecycle. In this repository,
`build_transport.prepare_transport_contribution` validates and prepares the currently
implemented transport-owned rows, and `build_transport.insert_transport_contribution` writes
them through a caller-owned connection. The standalone builder is the first client: it owns
schema creation, scenario economics, transaction control, validation, reporting, and atomic
publication. A future CANOE-main adapter should translate only cross-repository configuration,
use the same preparation/insertion contracts, and leave the shared database transaction and
compiler lifecycle with CANOE-main.

The present contribution contains the backend-owned technology and commodity templates only;
parameter-ready road and off-road artifacts are not yet assembled into SQLite rows. Do not
describe configured artifact routes as live database consumers until that wiring exists.

## Unsettled upstream seams

Do not freeze any of these into local modeling policy:

- `pipeline.py` does not yet process sector `fuel_imports`.
- `commercial/fuel_imports.py` is empty and `distribution/fuel/` has a CSV list but no
  completed linker builder. Transportation labels in that list are evidence to reconcile,
  not a stable commodity API.
- `common/naming.py` standardizes dataset codes only; it does not define shared technology or
  commodity naming.
- The upstream transportation data guide still describes the legacy spreadsheet compiler and
  does not define the new sector contract.
- The local `TransportationTechnology.notes` compatibility extension is not applied by the
  upstream initializer. Resolve that schema seam before targeting a shared database.
- CANOE-main's `GoldConnector` exposes data-lake cache artifacts, but no verified equivalent
  exists for this backend's source registry, provenance, and offline cache contracts.

These points were verified at the reviewed commit and may change.

## Configuration ownership

Until an adapter is implemented, CANOE-main and transportation configuration remain separate.
At integration time, classify every shared setting once:

- CANOE-main/base owns the shared database target and global model scope it initializes.
- Transportation owns transport sources, harmonization, representations, mappings,
  assumptions, and transport validation.
- The adapter owns only explicit translation or inheritance between those contracts.

Do not migrate transport acquisition or duplicate settings merely to resemble the current
commercial module.

## Upstream retrieval map

Inspect the branch head and only the files relevant to the change:

- `src/canoe/pipeline.py`: execution order and sector invocation.
- `src/canoe/common/module_interface.py`: sector lifecycle and output.
- `src/canoe/common/module_inheritance.py` and `src/canoe/sector_config.py`: inherited fields
  and sector registration.
- `src/canoe/initializer.py`: base-owned schema and rows.
- `src/canoe/commercial/config.py`, `build.py`, and `validation.py`: the current sector example.
- `src/canoe/common/db_tools.py`: transaction ownership.
- `src/canoe/common/fuels.py`, `common/naming.py`, and `distribution/fuel/`: unstable coupling
  and naming evidence.
- `src/canoe/common/cache_connector/`: data-lake cache behavior.
- `docs/what_is_canoe/model_architecture.md`: intended multi-sector framing.

## Questions for the eventual adapter

Before integration, re-establish:

1. the then-current sector registration, lifecycle, and output contract;
2. ownership of transaction boundaries and post-sector validation;
3. which base rows transport may require but must not duplicate;
4. resolution of the local technology-notes schema extension;
5. the implemented fuel/electricity linker and naming contract;
6. the exact mapping between global CANOE-main fields and transport scenario fields;
7. provenance expectations for transport data in the shared database.

Keep unanswered items in the adapter plan rather than implementing guesses.
