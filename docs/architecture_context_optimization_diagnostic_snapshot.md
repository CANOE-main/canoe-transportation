Repository inspection was read-only. No files were edited, no ExecPlan was created, and no tests were run because pytest initialization itself creates `.pytest-tmp/`.

The snapshot includes substantial uncommitted work: mapping/config/manual evidence, road aggregation, lifetime/bootstrap code, notebook/tests are modified; `vpic_model_years.py` and its test are untracked. Metrics and artifact counts below describe that current working tree, not `HEAD`.

## 1. Actual responsibilities and usage

| Directory | What currently lives there | Creators / consumers | Execution role |
|---|---|---|---|
| `src/validation/` | Eight Python files: typed config models, smoke wrapper, provenance objects, pinned schema integration, insertion, database checks, and narrow legacy comparison. | Called mainly by `src/build_transport.py`, `src/utils/`, `scripts/doctor.py`, and validation tests. | Partly default database build; several intended parameter-validation paths are test-only. |
| `src/fetching/` | Ten current Python files, including three vehicle-class adapters absent from architecture docs. Each substantive module is also a direct CLI. | Writes cache/interim artifacts. Only CER and StatCan are called by the default DAG. Road aggregation consumes NRCan/FuelEconomy evidence; road/lifetime/stock modules consume Ontario artifacts; bootstrap consumes vPIC outputs. | Mixed: two default, five standalone source adapters, three mapping-development adapters. |
| `tests/` | 23 current Python files, 199 `test_*` functions. Mostly fixture/unit tests, plus configuration, runtime-architecture, real-template, and artifact-backed integration checks. | Executed by pytest only; not part of Snakemake. `conftest.py` redirects temp files into a repository-local `.pytest-tmp/<pid>` directory. | Development validation. Some mapping tests consume real ignored interim artifacts rather than isolated fixtures. |
| `scripts/` | `doctor.py`, `clean_runtime.py`, and empty `__init__.py`. | The default DAG invokes `doctor.py`; cleanup is developer-invoked only. | Readiness/default orchestration plus development hygiene. |
| `inputs/validation/` | Exists but is empty, with no tracked files or code references. | No discovered creator or consumer. | None. |
| `outputs/validation/` | Four ignored files: one 1.86 MB notebook HTML and three mapping-review CSVs. The configured database-bootstrap JSON is absent. | Formal JSON would be written by `build_transport.py`; exact current filenames have no declared workflow producer or consumer. Bootstrap can write an arbitrary requested CSV via `--output`, but these filenames are not configured. | Currently functions as retained diagnostic/review scratch space more than a reproducible validation target. |

Key conflicts:

- Architecture documents `inputs/validation/` as “validation and parity reports,” but [paths.yaml](/C:/Users/rashi/ESM_databases/canoe-transportation/config/paths.yaml:8) defines no such input key; it defines only `outputs.validation` at [line 20](/C:/Users/rashi/ESM_databases/canoe-transportation/config/paths.yaml:20).
- The vehicle pipeline writes parameterization products into `inputs/1_interim/fetched_ontario_vehicle_population/`. That directory currently contains 69 files totaling about 382 MB: normalized source tables, mapping candidates, mapped stock, aggregation weights, survival outputs, bootstrap evidence, vPIC requests, and diagnostics. `inputs/2_processed/` currently contains zero files. Road aggregation explicitly resolves its output directory through the `interim` path at [road_aggregation.py:1569](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/road_aggregation.py:1569) and writes all products there at [line 1669](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/road_aggregation.py:1669).
- Documentation says `setup.py` fetches/caches sources and `build_transport.py` runs modules at [backend_architecture.md:27](/C:/Users/rashi/ESM_databases/canoe-transportation/docs/backend_architecture.md:27). Actual `setup.py` only performs configuration smoke validation, while the database build currently loads only technology and commodity templates.
- The default DAG consists of doctor, StatCan, CER, and template-database rules; see [Snakefile:27](/C:/Users/rashi/ESM_databases/canoe-transportation/workflow/Snakefile:27), [Snakefile:56](/C:/Users/rashi/ESM_databases/canoe-transportation/workflow/Snakefile:56), and [Snakefile:82](/C:/Users/rashi/ESM_databases/canoe-transportation/workflow/Snakefile:82). It does not orchestrate the vehicle pipeline.

## 2. `src/validation/` integrity

| Module | Real responsibility | Actual callers |
|---|---|---|
| `config_models.py` | Strict Pydantic models for paths, scenarios, sources, components, and DQ. | `src/utils` loads all YAML through these models; several fetchers and tests use individual source/component types. Runtime-critical. |
| `config_smoke.py` | Loads an already-validated bundle, re-runs cross-file validation, creates configured directories, gathers schema evidence, and returns status. | `src/setup.py` and `test_config.py`; not the default doctor/DAG. |
| `provenance.py` | Stable source IDs, data IDs, resolved/composite provenance, and registry row construction. | Default build uses only `source_id_mapping`; full resolution/composite/registry flow is exercised only by tests. |
| `schema_contract.py` | Verifies installed `canoe-schema` VCS provenance, obtains DDL, mutates it with the `technology.notes` extension, creates the schema, and performs preflight checks. | Database build, doctor, config smoke, and tests. |
| `insertion.py` | Homogeneous parameterized model insertion and provenance attachment to final parameter rows. | `insert_models` is used by the template build; `validate_parameter_rows` is test-only. |
| `database_bootstrap.py` | Post-insertion primary-key, provenance, FK, and SQLite integrity inspection. | Database build and tests. It does not bootstrap or publish a database. |
| `legacy_compare.py` | Set-based common-column comparison, excluding v4 provenance columns. | Conditional database build and tests; production caller compares only `technology` and `commodity`. |
| `__init__.py` | Empty package marker. | Imports only. |

Assessment: the intended config → provenance → schema → insertion → integrity/parity sequence exists as modules, but it is only partially integrated.

- Config ownership is split: structural models are in validation, while loading, cross-file validation, path resolution, and directory creation live in [utils/__init__.py:94](/C:/Users/rashi/ESM_databases/canoe-transportation/src/utils/__init__.py:94).
- Full provenance resolution and `validate_parameter_rows` form a tested future parameter-insertion seam, not a current default-build seam.
- The real database bootstrap, atomic publication, internal-template provenance, template parsing, and validation-report writing live in `build_transport.py`, especially [bootstrap_database](/C:/Users/rashi/ESM_databases/canoe-transportation/src/build_transport.py:329) and [write_validation_report](/C:/Users/rashi/ESM_databases/canoe-transportation/src/build_transport.py:451). `database_bootstrap.py` only validates an open connection.
- `schema_contract.py` does more than “compatibility”: it owns effective schema creation and a local DDL mutation at [schema_contract.py:185](/C:/Users/rashi/ESM_databases/canoe-transportation/src/validation/schema_contract.py:185).
- `config_smoke.py` has unclear/dead behavior: `load_config_bundle()` already raises when cross-file validation fails, so its returned `ok=False` branch is not the normal failure interface. It also checks for `"placeholder"` sources at [config_smoke.py:42](/C:/Users/rashi/ESM_databases/canoe-transportation/src/validation/config_smoke.py:42), while `SourceSpec.status` permits only `active` or `inactive` at [config_models.py:271](/C:/Users/rashi/ESM_databases/canoe-transportation/src/validation/config_models.py:271).
- Foreign-key enforcement is checked both before writes in `schema_contract` and after writes in `database_bootstrap`; that is duplicated but represents distinct pre/post boundaries.
- Configured parameter tolerances are not consumed by `legacy_compare`; the current parity implementation is a narrow exact set comparison.

## 3. Fetching/module drift

Current modules not correctly represented in `backend_architecture.md`:

- `fueleconomy_vehicles.py`: pinned FuelEconomy.gov class-evidence acquisition and normalization. It is absent from the documented tree.
- `vpic_vehicle_types.py`: gated vPIC vehicle-type evidence for classless mappings. Absent.
- `vpic_model_years.py`: request-scoped temporal corroboration. Absent and currently untracked.
- `nrcan_ceud.py` is present but described only as CEUD transport tables; it also owns fuel-consumption-rating request, cache, normalization, and publication logic beginning at [nrcan_ceud.py:59](/C:/Users/rashi/ESM_databases/canoe-transportation/src/fetching/nrcan_ceud.py:59).
- Conversely, the documented `vehicle_classes.py` at [backend_architecture.md:32](/C:/Users/rashi/ESM_databases/canoe-transportation/docs/backend_architecture.md:32) does not exist.

Vehicle-class responsibility overlap:

- `vehicle_population.py` owns MTO acquisition, source validation, Reports A/4/5 normalization, source-native key inventory, manifests, and a generic atomic CSV writer.
- `fueleconomy_vehicles.py`, `vpic_vehicle_types.py`, and `vpic_model_years.py` are separate source adapters with distinct physical/API contracts, cache shapes, request limits, offline behavior, and tests.
- `road_aggregation.py` owns both runtime mapping/application/weights and development candidate ranking/evidence catalogues.
- `vehicle_mapping_bootstrap.py` imports nine mapping/evidence helpers from road aggregation at [vehicle_mapping_bootstrap.py:17](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/vehicle_mapping_bootstrap.py:17), then adds manual arbitration, vPIC gates, range collapse, and replacement publication.
- `utils/vehicle_labels.py` is already the shared family-comparison layer used by both vPIC adapters, bootstrap, and road aggregation.
- `vpic_model_years.py` already reuses `VPicResponse` and `file_sha256` from `vpic_vehicle_types.py` at [vpic_model_years.py:23](/C:/Users/rashi/ESM_databases/canoe-transportation/src/fetching/vpic_model_years.py:23).

Wholesale consolidation under `fetching/vehicle_classes.py` is not structurally supported by the current boundaries: it would combine independent acquisition contracts with parameterization and maintainer inference. The demonstrated reuse is narrower—generic atomic writing/hashing, shared vPIC mechanics, and label normalization. There is also no current caller requiring a `vehicle_classes` façade.

## 4. Vehicle-mapping development overhead

### Normal family ETL, but not default Snakemake execution

- `vehicle_population.fetch_and_normalize`: source-native MTO fetching and normalization.
- `validate_vehicle_mapping`, `apply_vehicle_mapping`, `mapping_coverage`, aggregation-weight derivation, and mapped-stock publication in `road_aggregation.py`.
- `lifetimes_survival.py` independently reads and validates the reviewed mapping before attaching classes to transitions at [lifetimes_survival.py:1509](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/lifetimes_survival.py:1509).
- `stocks_and_demands.py` consumes the mapped-stock and lifetime artifacts at [stocks_and_demands.py:185](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/stocks_and_demands.py:185).

### Optional/development-only

- Entire `vehicle_mapping_bootstrap.py`; its module contract explicitly says this at [lines 1–4](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/vehicle_mapping_bootstrap.py:1).
- Both vPIC adapters. They consume bootstrap-produced request artifacts, and their outputs are optional when bootstrap loads them at [vehicle_mapping_bootstrap.py:826](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/vehicle_mapping_bootstrap.py:826).
- Manual-pass reconciliation and high-stock re-audit logic.
- The marimo notebook and its exported HTML.
- FuelEconomy evidence as currently used for candidate generation/cataloguing.
- Candidate ranking, candidate-dependent unresolved-reason attribution, latest unresolved worklist, and generated rating catalogues.

### Still hardwired despite development semantics

`build_road_aggregation_artifacts()` currently always:

1. Loads NRCan/FuelEconomy rating evidence.
2. Generates candidates.
3. Loads manual candidate evidence.
4. Produces candidate-based unresolved diagnostics and a rating catalogue.
5. Only then publishes mapped stock and weights.

That sequence is visible at [road_aggregation.py:1589](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/road_aggregation.py:1589). Thus candidate inference is outside the default DAG but remains mandatory in the ordinary direct road-aggregation command.

The current reviewed mapping has the finalized runtime shape: 2,029 rows, all `mto_crosswalk` and `reviewed`, with no rating-catalog rows. This agrees with the ownership note at [rules.yaml:227](/C:/Users/rashi/ESM_databases/canoe-transportation/config/parameters/rules.yaml:227). It establishes that mapping application and weight derivation do not require bootstrap, vPIC, manual candidate arbitration, or raw rating candidates. It does not establish whether the user considers the dirty worktree formally accepted.

### What default orchestration actually invokes

The default DAG invokes:

- Doctor.
- StatCan.
- CER.
- Template-only SQLite build and its validation report.

It invokes none of vehicle population, FuelEconomy, either vPIC adapter, road aggregation, lifetime derivation, stock derivation, or mapping bootstrap. The repository test explicitly locks out vehicle population from the Snakefile at [test_runtime_hygiene.py:117](/C:/Users/rashi/ESM_databases/canoe-transportation/tests/test_runtime_hygiene.py:117).

One mapping-development dependency does leak into default execution: doctor calls `validate_manual_registry()` at [doctor.py:133](/C:/Users/rashi/ESM_databases/canoe-transportation/scripts/doctor.py:133), and that function validates every registered manual CSV without filtering to scenario-active sources at [manual_parameters.py:143](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/manual_parameters.py:143). Consequently the default doctor requires and validates the 1,887-row `mapped_mto_make_model_keys.csv`, even though bootstrap itself is not invoked.

## 5. Complexity and reuse evidence

Approximate LOC excludes blank and full-line comment lines but includes docstrings.

| Scope | Files | Physical lines | Approx. Python LOC |
|---|---:|---:|---:|
| `src/` | 28 | 17,182 | 15,871 |
| `tests/` | 23 | 7,389 | 6,465 |
| Vehicle slice: 9 source modules | 9 | 8,913 | 8,369 |
| Vehicle slice: 9 tests | 9 | 3,055 | 2,661 |
| Vehicle marimo notebook | 1 | 3,090 | 2,885 |
| Complete defined vehicle slice | 19 | 15,058 | 13,915 |

The vehicle slice accounts for about 53% of current `src` LOC and 41% of test LOC.

Largest source modules:

1. `vehicle_mapping_bootstrap.py` — 1,953 lines.
2. `vehicle_population.py` — 1,825.
3. `lifetimes_survival.py` — 1,715.
4. `road_aggregation.py` — 1,706.
5. `nlr_atb_autonomie.py` — 1,625.

Largest tests are `test_assorted_sources.py` at 902 lines and `test_vehicle_mapping_bootstrap.py` at 882. The mapping test contains artifact-backed checks that read the real reviewed mapping, current stock, and bootstrap evidence at [test_vehicle_mapping_bootstrap.py:734](/C:/Users/rashi/ESM_databases/canoe-transportation/tests/test_vehicle_mapping_bootstrap.py:734).

Concrete duplication/reuse evidence:

- `find_repo_root` is repeated nearly verbatim in `src/utils`, `scripts/doctor.py`, and `scripts/clean_runtime.py`.
- File hashing is independently implemented in at least `assorted_sources`, CER, FuelEconomy, NLR, vehicle population, vPIC, and `build_transport`.
- Atomic dataframe/text publication is independently implemented in FuelEconomy, NRCan, and vehicle population. Four parameterization modules and both vPIC adapters import the generic writer from `fetching.vehicle_population`, creating a layer dependency on an unrelated source adapter.
- `_quote_identifier` is duplicated in `build_transport.py` and `validation/database_bootstrap.py`.
- `normalize_vehicle_text` in road aggregation and `normalize_vehicle_label` in `utils.vehicle_labels` are overlapping normalization concepts with different edge behavior; bootstrap uses both layers.
- The two vPIC modules repeat request/cache/manifest/error-loop structures, while already sharing the response model, hash helper, and family-equivalence function.
- `scripts/doctor.py` overlaps with `src/setup.py`/`validation.config_smoke`: both load and validate config and gather schema evidence, but only doctor is orchestrated.
- Three different mapping-candidate snapshots currently coexist:

  - Reviewed config: 2,029 rows.
  - Interim `vehicle_size_class_map_candidate.csv`: 1,078 rows.
  - `outputs/validation/vehicle_size_class_map_candidate.csv`: 2,059 rows.

  They have different hashes and no shared declared artifact contract.
- Ignored `__pycache__` files are present beneath source, tests, and scripts; `clean_runtime.py` handles root runtime globs but does not enumerate nested bytecode directories.

## 6. Structural discrepancies

| Area | documented intent | actual implementation | issue/risk | evidence/files |
|---|---|---|---|---|
| Fetching inventory | `vehicle_classes.py` represents additional class evidence | No such module; three source-specific class/vPIC adapters exist | Architecture tree is not a reliable module inventory | [backend architecture](/C:/Users/rashi/ESM_databases/canoe-transportation/docs/backend_architecture.md:29), `src/fetching/*.py` |
| Setup/build orchestration | Setup fetches and validates sources; build runs modules | Setup is config smoke only; build loads technology/commodity templates only | “Default backend execution” is ambiguous without reading the DAG and build code | [setup.py](/C:/Users/rashi/ESM_databases/canoe-transportation/src/setup.py:36), [build_transport.py](/C:/Users/rashi/ESM_databases/canoe-transportation/src/build_transport.py:79) |
| Default vehicle execution | Active MTO source and parameter modules imply pipeline participation | Default DAG invokes no vehicle fetching or parameterization | Scenario/source activation does not imply orchestration coverage | [scenario](/C:/Users/rashi/ESM_databases/canoe-transportation/config/scenarios/legacy_reproduction.yaml:26), [Snakefile](/C:/Users/rashi/ESM_databases/canoe-transportation/workflow/Snakefile:27) |
| Mapping-development boundary | Final reviewed CSV is runtime authority; inference is development-only | Direct road aggregation still always regenerates candidates and candidate diagnostics | Development cost remains coupled to an ordinary family entrypoint | [rules.yaml](/C:/Users/rashi/ESM_databases/canoe-transportation/config/parameters/rules.yaml:229), [road_aggregation.py](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/road_aggregation.py:1561) |
| Default doctor | Development bootstrap is absent from default ETL | Doctor still validates every manual source, including MTO candidate evidence | Final mapping does not remove manual-candidate file from default readiness | [doctor.py](/C:/Users/rashi/ESM_databases/canoe-transportation/scripts/doctor.py:133), [manual_parameters.py](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/manual_parameters.py:149) |
| Validation boundary | Validation package owns config, provenance, schema, insertion, integrity, parity, reports | Loader/cross-file validation is in utils; bootstrap/publication/report writing is in build; parameter provenance/insertion is test-only | Intended sequence exists but ownership and runtime coverage differ | [utils](/C:/Users/rashi/ESM_databases/canoe-transportation/src/utils/__init__.py:94), [build](/C:/Users/rashi/ESM_databases/canoe-transportation/src/build_transport.py:329) |
| `database_bootstrap.py` | Integrity and publication checks | Only connection validation; publication is in `build_transport.py` | Filename/documentation overstates module ownership | [database_bootstrap.py](/C:/Users/rashi/ESM_databases/canoe-transportation/src/validation/database_bootstrap.py:14) |
| Input artifact layers | Interim is normalized/auditable; processed is parameter-ready | All vehicle source, mapping, weights, survival, and diagnostic products share one interim fetch directory; processed is empty | Ownership and impact routing cannot be inferred from layer/path | [road outputs](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/road_aggregation.py:1669), [lifetime outputs](/C:/Users/rashi/ESM_databases/canoe-transportation/src/parameterization/lifetimes_survival.py:1669) |
| Validation directories | Both input parity reports and output integrity reports are documented | `inputs/validation` is empty/unconfigured; `outputs/validation` contains undeclared mapping scratch artifacts while configured JSON is absent | Two apparent validation homes, only one configured, with unclear artifact lifecycle | [backend architecture](/C:/Users/rashi/ESM_databases/canoe-transportation/docs/backend_architecture.md:57), [paths.yaml](/C:/Users/rashi/ESM_databases/canoe-transportation/config/paths.yaml:17) |
| Diagnostic artifact identity | Named outputs should have deterministic ownership | Three differently sized/hashes mapping candidate snapshots exist in config/interim/output layers | Context consumers can select stale evidence by filename alone | [reviewed map](/C:/Users/rashi/ESM_databases/canoe-transportation/config/parameters/vehicle_size_class_map.csv:1), [output candidate](/C:/Users/rashi/ESM_databases/canoe-transportation/outputs/validation/vehicle_size_class_map_candidate.csv:1) |
| Architecture reference status | Structural ownership reference | Lists missing parameter modules, missing `vehicle_classes.py`, and broader setup/build behavior than exists | Reference behaves partly as target architecture rather than current impact map | [backend architecture](/C:/Users/rashi/ESM_databases/canoe-transportation/docs/backend_architecture.md:26) |

<oai-mem-citation>
<citation_entries>
MEMORY.md:68-75|note=[prior vehicle mapping boundaries used to target inspection and verified against current files]
MEMORY.md:246-251|note=[context ownership notes used to interpret architecture reference role]
</citation_entries>
<rollout_ids>
019fc9c3-d858-7ab2-b80c-3f7fde249fa4
019f8d29-9bb4-77e1-8144-b1a13bfc6c3c
</rollout_ids>
</oai-mem-citation>
