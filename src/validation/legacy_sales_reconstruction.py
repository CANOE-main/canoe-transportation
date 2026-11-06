"""Read-only replay of the supplied sales-cohort notebook's vehicle recipe.

The notebook's active compiler call and inspected function bodies are authoritative.
Only function definitions are executed: notebook I/O and CSV/SQLite writes never run.
The inherited database is an explicit input snapshot, distinct from disputed outputs.
"""

import ast
from contextlib import closing
import hashlib
import json
from math import isclose
from pathlib import Path

import numpy as np
import pandas as pd

from utils import file_sha256
from validation.sqlite_utils import open_sqlite_readonly


# Inspected executable recipe identities, independent of formatting and saved outputs.
RECIPE_AST = {
    "organize_activity_ceud": "85d794ab6859f756023162a426013531398023134ce6463aac07db71f3ed3af6",
    "shift_time_period": "c55c6f7c94345d78b00c4a0cb97c3a2a44bccfae687143c320ce9b0687230ca3",
    "compile_data_existing_capacity": "29d0772947c90ebe6d931b2c2ab8d9652d70dee1692bc7ed06a6e6021844fbec",
    "compile_data_eff_cova": "03514c7e28455fcdde6f4804ea6f4cf15e6e8ea21fa059c803ad8915de732ca5",
    "compile_data_survival_lifetime_transport": "39ea6cb060f9bfc6147c1ce1b597746378c7fcf7af65e9e6b26eae64aac02967",
    "compile_data_c2a": "c5fe60f9842b758069b9c0e80d8e35331d418f875feaf9d229f018720d9d10cd",
    "compile_data_lacf_v_ex": "15e5e4237920340918fd3d296d6dc0dd819bb67a8b1a902f77de5156ecd42910",
    "compile_data_lacf_v_n": "619e75d1095bccaae1880e347fae8065eb3686b5a9668fc540310e525fc3e34c",
    "compile_lacf_transport": "d6c308e736f9279f2ba5a71f72490eb0749a94a3677a4a9a4ce0bd62124b3616",
    "remove_data_noexca": "f4d94fbd49a22168ad922b5afa954a976d2064e760db3f50c57c9b44f8097538",
    "main_compile_data_transport": "2c975dd610cea165674604d297fbd7301286179a58adeff0939f70efb3a09d71",
}

SERVICE_CLASSES = {
    "T_D_pkm_ldv_c": "cars", "T_D_pkm_ldv_t": "passenger_light_trucks",
    "T_D_tkm_ldv_t": "freight_light_trucks", "T_D_tkm_mdv_t": "medium_trucks",
    "T_D_tkm_hdv_t": "heavy_trucks",
}
VEHICLE_PREFIXES = ("T_LDV_C", "T_LDV_LTP", "T_LDV_LTF", "T_MDV_T", "T_HDV_T")


def _unique(frame, keys, label):
    if frame[keys].isna().any().any() or frame.duplicated(keys).any():
        raise ValueError(f"Legacy reconstruction has missing/duplicate {label}")


def _validate_legacy_physical(stock: pd.DataFrame) -> None:
    """Reject unsupported units/physical inputs without modifying native factors."""
    if stock.empty or not stock.units.map(
        lambda value: str(value).strip("() ")
    ).isin({"k units", "k vehicles"}).all():
        raise ValueError("Legacy vehicle capacity must be nonempty and use thousand vehicles")
    required = ["raw_capacity_k_vehicles", "surviving_k_vehicles", "c2a", "utilization", "efficiency",
                "calibration_survival_fraction", "baseline_survival_fraction", "lower_bound", "upper_bound"]
    if stock[required].isna().any().any() or not np.isfinite(stock[required]).all().all():
        raise ValueError("Incomplete or nonfinite legacy physical parameter support")
    if (stock.raw_capacity_k_vehicles.lt(0).any() or stock.surviving_k_vehicles.lt(0).any()
            or stock.c2a.le(0).any() or stock.efficiency.le(0).any()
            or stock.utilization.lt(0).any() or stock.lower_bound.lt(0).any()
            or stock.lower_bound.gt(stock.utilization).any()
            or stock.upper_bound.lt(stock.utilization).any()):
        raise ValueError("Legacy physical parameters or utilization bounds are inadmissible")
    if not stock[["calibration_survival_fraction", "baseline_survival_fraction"]].map(
        lambda value: 0 <= value <= 1
    ).all().all():
        raise ValueError("Legacy survival fractions must lie in [0,1]")


def _inspected_recipe(notebook: Path):
    document = json.loads(notebook.read_text(encoding="utf-8"))
    nodes = [node for cell in document["cells"] if cell["cell_type"] == "code"
             for node in ast.parse("".join(cell.get("source", []))).body]
    functions = {node.name: node for node in nodes if isinstance(node, ast.FunctionDef)}
    for name, identity in RECIPE_AST.items():
        if name not in functions or hashlib.sha256(
            ast.dump(functions[name], include_attributes=False).encode()
        ).hexdigest() != identity:
            raise ValueError(f"Legacy executable recipe changed: {name}; inspect before replay")
    calls = [node for node in nodes if isinstance(node, ast.Expr)
             and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
             and node.value.func.id == "main_compile_data_transport"]
    if len(calls) != 1:
        raise ValueError("Legacy recipe must contain one explicit active compiler call")
    arguments = {item.arg: item.value for item in calls[0].value.keywords}
    if (ast.unparse(arguments["range_vin"]) != "np.arange(2005, 2026, 1)"
            or ast.unparse(arguments["range_period"]) != "range(2025, 2046, 5)"
            or ast.literal_eval(arguments["fixed_lifetime"]) is not False
            or ast.literal_eval(arguments["growth_rate_type"]) != "gdp"):
        raise ValueError("Legacy active compiler arguments changed; inspect before replay")
    return functions, {name: ast.unparse(value) for name, value in arguments.items()}


def _recipe_functions(functions):
    captures = {}

    def capture(name, frame):
        captures[name] = frame.copy(deep=True)

    # These observations do not change any source expression or calculation.
    stock = functions["compile_data_existing_capacity"]
    observed = []
    for statement in stock.body:
        source = ast.unparse(statement)
        if source.startswith("exca_update_v = exca_update_v.groupby("):
            observed.extend(ast.parse("_capture('annual', exca_update_v)").body)
        if source.startswith("exca_update_v = exca_update_v[['region', 'tech', 'vintage', 'capacity', 'units']]"):
            observed.extend(ast.parse("_capture('mapped', exca_update_v)").body)
        observed.append(statement)
    stock.body = observed
    safe_functions = [node for name, node in functions.items()
                      if name in RECIPE_AST and name != "main_compile_data_transport"]
    module = ast.fix_missing_locations(ast.Module(body=safe_functions, type_ignores=[]))
    namespace = {"pd": pd, "np": np, "_capture": capture}
    exec(compile(module, "<inspected legacy vehicle recipe>", "exec"), namespace)
    return namespace, captures


def reconstruct_legacy_sales(
    notebook: Path, inherited_database: Path, *, comparison_year: int, regions: list[str],
) -> tuple[dict[str, pd.DataFrame], dict]:
    """Replay independent stock, representative survival, UF and input energy.

    Inherited Efficiency/CostVariable/ExistingCapacity/LifetimeTech are read only.
    Missing original input identity remains an evidence gap, never an implicit match.
    """
    functions, arguments = _inspected_recipe(notebook)
    notebook_identity, inherited_identity = file_sha256(notebook), file_sha256(inherited_database)
    namespace, captures = _recipe_functions(functions)
    identities = []

    def workbook(name):
        path = notebook.parent / name
        if not path.is_file():
            raise ValueError(f"Missing supplied legacy workbook: {path}")
        identities.append({"path": str(path), "sha256": file_sha256(path)})
        return pd.read_excel(path)

    sales, shares = workbook("vehicle_sales_by_size.xlsx"), workbook("vehicle_sales_by_powertrain.xlsx")
    survival_input, c2a_input = workbook("survival_curve_transport.xlsx"), workbook("c2a_vehicle.xlsx")
    activity_input = workbook("activity.xlsx")
    if not sales.Unit.eq("k units").all() or not activity_input.Unit.isin({"M pkm", "M tkm"}).all():
        raise ValueError("Legacy workbook stock/activity units differ from the inspected conversion recipe")
    stock, historical = namespace["compile_data_existing_capacity"](sales, shares, np.arange(2005, 2026, 1))
    with closing(open_sqlite_readonly(inherited_database)) as connection:
        defaults = {table: pd.read_sql_query(f"SELECT * FROM {table}", connection)
                    for table in ["Efficiency", "CostVariable", "ExistingCapacity", "LifetimeTech",
                                  "CapacityToActivity", "Technology", "LimitAnnualCapacityFactor"]}
    transport_tech = set(defaults["Technology"].loc[defaults["Technology"].sector.eq("transportation"), "tech"])
    for table in set(defaults) - {"Technology"}:
        defaults[table] = defaults[table].loc[defaults[table].tech.isin(transport_tech)].copy()
    period_range = range(2025, 2046, 5)
    shifted = {}
    for table in ["Efficiency", "CostVariable", "ExistingCapacity"]:
        source = defaults[table].loc[defaults[table].tech.str.startswith(VEHICLE_PREFIXES)].copy()
        shifted[table] = namespace["shift_time_period"](source, period_range)
    lifetime = defaults["LifetimeTech"].loc[defaults["LifetimeTech"].tech.str.startswith(VEHICLE_PREFIXES)].copy()
    lifetime["tech"] = lifetime.tech.str.replace(r"_(?:EX|N)$", "", regex=True)
    lifetime = lifetime.drop_duplicates()
    technology, _, efficiency, variable = namespace["compile_data_eff_cova"](
        shifted["ExistingCapacity"][["region", "vintage", "tech"]].drop_duplicates(),
        historical, shifted["Efficiency"], shifted["CostVariable"], period_range,
    )
    curves, lifetimes, variable = namespace["compile_data_survival_lifetime_transport"](
        survival_input, technology, variable, fixed_lifetime=False, lifetime_default_v=lifetime,
    )
    other_c2a = defaults["CapacityToActivity"].loc[~defaults["CapacityToActivity"].tech.str.startswith(VEHICLE_PREFIXES)].copy()
    _, c2a = namespace["compile_data_c2a"](c2a_input, other_c2a)
    activity = namespace["organize_activity_ceud"](activity_input)
    bounds = namespace["compile_data_lacf_v_ex"](stock, curves, c2a, efficiency, activity)
    new_bounds = namespace["compile_data_lacf_v_n"](stock, curves, c2a, efficiency, activity)
    shifted_bounds = namespace["shift_time_period"](defaults["LimitAnnualCapacityFactor"], period_range)
    shifted_efficiency = namespace["shift_time_period"](defaults["Efficiency"], period_range)
    final_bounds = namespace["compile_lacf_transport"](
        bounds, new_bounds,
        shifted_bounds.loc[~shifted_bounds.tech.str.startswith(VEHICLE_PREFIXES)],
        shifted_efficiency.loc[~shifted_efficiency.tech.str.startswith(VEHICLE_PREFIXES)],
    )
    before_finalize = bounds.copy()
    bounds = final_bounds.loc[final_bounds.tech.str.startswith(VEHICLE_PREFIXES)
                              & final_bounds.vintage.lt(min(period_range))].drop(columns="data_id", errors="ignore")
    comparison_columns = ["region", "tech", "vintage", "output_comm", "operator", "factor"]
    pd.testing.assert_frame_equal(
        before_finalize[comparison_columns].sort_values(comparison_columns).reset_index(drop=True),
        bounds[comparison_columns].sort_values(comparison_columns).reset_index(drop=True),
        check_dtype=False,
    )
    # Preserve the source cleanup contracts after the complete calculation sequence.
    efficiency = namespace["remove_data_noexca"](efficiency, period_range, stock)
    curves = namespace["remove_data_noexca"](curves, period_range, stock)
    bounds = namespace["remove_data_noexca"](bounds, period_range, stock)
    keys = ["region", "tech", "vintage"]
    service = efficiency.loc[efficiency.output_comm.isin(SERVICE_CLASSES)].copy()
    _unique(service, keys, "vehicle service efficiency")
    stock = stock.loc[stock.region.isin(regions)].copy()
    stock = stock.merge(service[keys + ["input_comm", "output_comm", "efficiency"]],
                        on=keys, how="left", validate="one_to_one")
    stock["road_class"] = stock.output_comm.map(SERVICE_CLASSES)
    if stock[["efficiency", "road_class"]].isna().any().any():
        raise ValueError("Selected inherited input does not support every recipe vehicle edge")
    stock = stock.rename(columns={"capacity": "raw_capacity_k_vehicles"})
    for year, column in [(2023, "calibration_survival_fraction"), (comparison_year, "baseline_survival_fraction")]:
        selected = curves.loc[curves.period.eq(year), keys + ["fraction"]].rename(columns={"fraction": column})
        _unique(selected, keys, "representative-vintage survival")
        stock = stock.merge(selected, on=keys, how="left", validate="one_to_one")
        if stock[column].isna().any():
            raise ValueError(f"Legacy survival does not cover comparison/calibration year {year}")
    stock["surviving_k_vehicles"] = stock.raw_capacity_k_vehicles * stock.baseline_survival_fraction
    stock["calibration_stock_k_vehicles"] = stock.raw_capacity_k_vehicles * stock.calibration_survival_fraction
    factors = bounds.pivot(index=keys + ["output_comm"], columns="operator", values="factor").reset_index()
    factors["utilization"] = (factors["ge"] + factors["le"]) / 2
    factors = factors.rename(columns={"ge": "lower_bound", "le": "upper_bound"})
    stock = stock.merge(factors, on=keys + ["output_comm"], how="left", validate="one_to_one")
    _unique(c2a, ["region", "tech"], "capacity-to-activity factors")
    stock = stock.merge(c2a[["region", "tech", "c2a"]], on=["region", "tech"], how="left", validate="many_to_one")
    _validate_legacy_physical(stock)
    stock["implied_activity_bn"] = stock.surviving_k_vehicles * stock.c2a * stock.utilization
    stock["implied_vehicle_input_pj"] = stock.implied_activity_bn / stock.efficiency
    stock["baseline_year"] = comparison_year
    stock["calibration_year"] = 2023
    stock["evidence"] = "Legacy recipe"

    annual = captures["annual"].rename(columns={"Region": "region", "Year": "vintage_year", "sales": "raw_capacity_k_vehicles"})
    mapped = captures["mapped"][["region", "Mode", "Powertrain", "vintage", "tech"]]
    annual = annual.merge(mapped, on=["region", "Mode", "Powertrain", "vintage"], how="inner", validate="many_to_one")
    annual = annual.merge(stock[keys + ["road_class", "calibration_survival_fraction", "baseline_survival_fraction"]],
                          on=keys, how="inner", validate="many_to_one")
    annual["surviving_k_vehicles"] = annual.raw_capacity_k_vehicles * annual.baseline_survival_fraction
    annual["annual_position"] = annual.vintage_year - annual.vintage
    annual["observation_status"] = np.where(annual.vintage_year.gt(2023), "2023 product held constant", "Workbook product")
    annual["boundary_exception"] = np.where(annual.vintage.eq(2000), "2005 only; requested support begins 2005", "")
    annual["evidence"] = "Legacy recipe"
    annual = annual.drop(columns=["Mode", "Powertrain", "Unit"])
    sums = annual.groupby(keys)[["raw_capacity_k_vehicles", "surviving_k_vehicles"]].sum()
    for row in stock.itertuples():
        for column in ["raw_capacity_k_vehicles", "surviving_k_vehicles"]:
            if not isclose(sums.loc[(row.region, row.tech, row.vintage), column], getattr(row, column), rel_tol=1e-12, abs_tol=1e-9):
                raise ValueError("Legacy annual lineage does not recover model-vintage capacity")
    class_keys = ["region", "road_class"]
    calibration = stock.groupby(class_keys, as_index=False).agg(
        calibration_stock_k_vehicles=("calibration_stock_k_vehicles", "sum"),
        capacity_activity_denominator_bn=("calibration_stock_k_vehicles", lambda values: float((values * stock.loc[values.index, "c2a"]).sum())),
        c2a=("c2a", "first"), utilization=("utilization", "first"),
        lower_bound=("lower_bound", "first"), upper_bound=("upper_bound", "first"),
    )
    for _, group in stock.groupby(class_keys):
        if any(group[column].nunique() != 1 for column in ["c2a", "utilization", "lower_bound", "upper_bound"]):
            raise ValueError("Legacy recipe unexpectedly has multiple class utilization/c2a values")
    observed = activity.loc[activity.period.eq(2023)].copy()
    observed["road_class"] = observed.commodity.map(SERVICE_CLASSES)
    calibration = calibration.merge(observed[class_keys + ["demand"]].rename(columns={"demand": "calibration_activity_bn"}),
                                    on=class_keys, how="left", validate="one_to_one")
    if not np.allclose(calibration.utilization * calibration.capacity_activity_denominator_bn,
                       calibration.calibration_activity_bn, rtol=1e-12, atol=1e-9):
        raise ValueError("Legacy 2023 utilization calibration identity failed")
    calibration["calibration_year"], calibration["baseline_year"] = 2023, comparison_year
    totals = stock.groupby(class_keys, as_index=False)[[
        "raw_capacity_k_vehicles", "surviving_k_vehicles", "implied_activity_bn", "implied_vehicle_input_pj",
    ]].sum()
    totals["baseline_year"], totals["evidence"] = comparison_year, "Legacy recipe"
    context = pd.DataFrame([
        {"stage": "Active call", "interpretation": "fixed_lifetime=False; workbook survival, not filename or saved fixed-lifetime output"},
        {"stage": "Acquisition", "interpretation": "Annual class sales × shares; missing products zero; 2023 products held through2025; no CEUD stock scaling"},
        {"stage": "Aggregation", "interpretation": "2005→2000; 2006–10→2005; 2011–15→2010; 2016–20→2015; 2021–25→2020; sum partial bins"},
        {"stage": "Survival", "interpretation": "Sum acquisitions first, then rounded workbook survival at calendar year minus representative model vintage; apply once"},
        {"stage": "Calibration", "interpretation": "2023 historical class activity / Σ(raw vintage capacity × survival(2023−vintage) × own c2a); includes projected2024–25 acquisitions in vintage2020"},
        {"stage": "Bounds", "interpretation": "Central calibration ×0.975 and ×1.025; parameters constrain solver operation, they are not realized dispatch"},
        {"stage": "Energy", "interpretation": "Σ(surviving2025 stock × own c2a × central UF / inherited shifted/interpolated service efficiency); vehicle input, PHEV blend once, excludes upstream charger losses"},
        {"stage": "Inherited input", "interpretation": f"Explicit read-only snapshot {inherited_database}; named original Input data/canoe_ALL_cm.sqlite unavailable; historical identity unproven"},
    ])
    original_path = notebook.parent / ast.literal_eval(arguments["canoe_default_all"])
    original_available = original_path.is_file()
    original_identity_matches = original_available and file_sha256(original_path) == inherited_identity
    if (file_sha256(notebook) != notebook_identity or file_sha256(inherited_database) != inherited_identity
            or any(file_sha256(Path(item["path"])) != item["sha256"] for item in identities)):
        raise ValueError("Legacy reference identity changed during read-only reconstruction")
    metadata = {
        "notebook": {"path": str(notebook), "sha256": notebook_identity},
        "inherited_database": {"path": str(inherited_database), "sha256": inherited_identity},
        "workbooks": identities, "active_arguments": arguments,
        "recipe_ast_sha256": RECIPE_AST, "original_inherited_input_available": original_available,
        "reconstruction_implementation_sha256": file_sha256(Path(__file__)),
        "original_named_input": str(original_path), "original_input_identity_matches": original_identity_matches,
        "calibration_year": 2023, "comparison_year": comparison_year,
        "final_lacf_preserves_existing_calibration": True, "references_immutable_during_replay": True,
        "evidence_gap": ("" if original_identity_matches else
                         "Original named all-region input database identity unverified; explicit inherited snapshot does not establish historical equivalence"),
    }
    return {
        "legacy_reconstructed_annual": annual.reset_index(drop=True),
        "legacy_reconstructed_parameters": stock.reset_index(drop=True),
        "legacy_reconstructed_utilization": calibration,
        "legacy_reconstructed_energy_detail": stock.copy(),
        "legacy_reconstructed_totals": totals,
        "legacy_reconstruction_context": context,
        "legacy_reconstructed_survival": curves.loc[curves.region.isin(regions)].reset_index(drop=True),
        "legacy_reconstructed_lifetimes": lifetimes.loc[lifetimes.region.isin(regions)].reset_index(drop=True),
    }, metadata
