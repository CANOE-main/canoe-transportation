# Integrate transportation with CANOE-main

CANOE-main coordinates multiple sectors in one model database. The transportation
backend prepares the same demand, capacity, efficiency, lifetime, cost and
provenance rows for a standalone build or a caller-owned database. A CANOE-main
transport module can use that existing Python API while the compiler owns the
shared database and its publication. The reference `unstable` branch provides
the sector interface and compiler sequence. Transport registration is a separate
adapter step. [CANOE-main compiler][pipeline] · [Sector interface][module-interface]

## Align configuration and ownership

| Confirm before preparing transport | Why it matters |
| --- | --- |
| **Regions.** Agree the geographic scope and output codes. | Transport emits `NLLAB` and `PEI` for `NL` and `PE`. Its `BCT` scope includes the territories, so a mapping to CANOE-main's `BC` needs an explicit coverage decision. |
| **Periods and economics.** Match existing vintages, model periods, period step and evaluation years. Use the compiler's currency year and rates. | CANOE-main appends a terminal period for Temoa. Keep it outside the transport model periods. Align opening or closing conditions explicitly so shared period labels refer to the same years. |
| **Schema and shared rows.** Use compatible CANOE v4 row models and ensure `technology.notes` is present. | [`apply_technology_notes_extension`](../../src/validation/schema_contract.py) supplies the approved column where needed. Agree ownership of labels, commodities, emission gases and datasets. Distinct dataset IDs can coexist under the same commodity name, so check the units, flags and physical meaning of each shared definition. |
| **Transport inputs and configuration.** Supply the locked environment, registered source caches, reviewed tables and external-model outputs. | The transport scenario owns the source editions and sector methods. Keep those choices explicit when inheriting shared settings. Serialize preparation or isolate all configured artifact paths. Two database filenames can still share mutable preparation files. |

See the [scenario guide](../scenario_configuration.md) and
[CANOE-main integration reference](../canoe_main_orchestrator.md) for inheritance,
schema dependencies, and the upstream source references.

## Prepare and insert in the sector stage

CANOE-main initializes the schema, regions, periods, time slices and shared emission
commodities before it runs end-use sectors. In a transport adapter, use the
following sequence from [`build_transport.py`](../../src/build_transport.py).

1. Call `load_config_bundle(scenario_path, repo_root=transport_root)` to validate
   the scenario and resolve the transport checkout independently of the compiler's
   working directory. Replay the registered prerequisites with
   `prepare_scenario_inputs(bundle, download=False)`.
2. Use the initialized connection and configured template directory in
   `prepare_transport_contribution(connection, bundle=bundle, template_dir=template_dir)`.
   Obtain the directory with `resolve_input_path(bundle, "template")`. Preparation
   reads the caller's time axes, can write configured evidence files, and leaves
   SQLite unchanged.
3. Enable foreign keys before starting the caller's transaction. Insert via
   `insert_transport_contribution(connection, contribution, conflict="error")`
   inside that transaction. It rechecks the prepared rows and registers
   their provenance. Use `ignore_identical` only for agreed identical shared rows.
   Check the returned transport keys, provenance and database integrity before
   committing. If using the [database validator](../../src/validation/database_bootstrap.py)
   for exact key comparisons, include all expected keys in each checked shared table.

The adapter implements `run()`, `get_sector()` and `get_dataset_code()`. It
identifies its sector as `CANOESector.Transportation` (`TRP`). Its `run()`
returns `CANOEModuleOutput` with fuel imports and emission declarations. The
compiler uses those outputs to build electricity and fuel supply, then finalizes
emissions before representative-period reduction and Temoa. [Sector registration][registration] · [CANOE-main compiler][pipeline]

## Connect supply, emissions and time

- **Supply.** Derive imports and consuming regions from the prepared efficiency
  paths. Connect upstream electricity to `T_elc` and preserve the link from
  time-resolved supply through charging to annual vehicle service. Reconcile the
  blend meanings of liquid fuel commodities and the connection from upstream
  hydrogen to `T_h2_700` and the vehicle refueling pathways. Matching a commodity
  name alone leaves its supply and accounting assumptions unresolved.
- **Emissions.** Agree responsibility for upstream and combustion gas activities
  before central CO₂-equivalent calculation. Keep GREET `emission_embodied` rows
  on their vehicle-cycle basis. CANOE-main's activity finalizer handles a different
  table and accounting boundary.
- **Time.** Profiled LDV charging uses inherited slice labels or a complete
  `charging_profiles.time_mapping` for all 8,760 elapsed hours. The
  representative-day reducer retains selected hourly factors and changes their
  weights. Check the weighted mean against the selected utilization target.
  If it changes, assess rescaling with the final weights and recheck factor bounds
  and the annual utilization floor. Explicit charger representations use annual
  constraints and require profiles to be disabled. [Charging and representative time](charging.md#representative-time-can-change-the-annual-mean) · [CANOE-main reducer][reducer]

Start validation with an upstream-initialized database for one region and a short
period grid. Check rollback, supported transport keys, shared rows and carrier
continuity, then exercise the chosen charging and time representation. Use an
integrated Temoa solve with the configured supply to check feasibility.

[pipeline]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/pipeline.py
[module-interface]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/common/module_interface.py
[registration]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/sector_config.py
[reducer]: https://github.com/CANOE-main/CANOE/blob/be2235d6fa7275d0a183fe5f89c54b88d67f0f35/src/canoe/representative_periods/build.py
