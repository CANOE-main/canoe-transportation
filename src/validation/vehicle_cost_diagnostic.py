"""Opt-in, recorded vehicle-cost diagnostics; readers never prepare model inputs.

Capture a successful build and its cost audit explicitly. Source-class curves reuse
the production price selector, currency converter and empirical M&R functions.
They are diagnostic calculations, not additional SQLite parameter rows.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import textwrap
from uuid import uuid4

import numpy as np
import pandas as pd

from fetching.nlr_atb_autonomie import configured_trajectory
from parameterization.currency import CerCurrencyConverter
from parameterization.ldv_ev_ranges import (
    bucket_range_evidence, module_rules, validate_share_evidence,
)
from parameterization.road_capex_opex import (
    bean_maintenance_per_mile, ldv_repair_per_mile, select_nlr_purchase_prices,
)
from utils import (
    ConfigBundle, file_sha256, find_repo_root, load_config_bundle,
    load_harmonization_rules, load_yaml, resolve_artifact_path,
    resolve_configured_path, resolve_input_path, resolve_parameter_path,
    resolve_repo_path, write_dataframe_atomic, write_text_atomic,
)
from validation.charging_cost_comparison import verify_build_snapshot
from validation.config_models import PathsConfig
from validation.notebook_figures import identified_chart
from validation.provenance import resolve_provenance


ROUTE = "vehicle_cost_diagnostic"
ROAD_MODES = {
    "cars": "Cars", "passenger_light_trucks": "Passenger light trucks",
    "freight_light_trucks": "Freight light trucks", "medium_trucks": "Medium trucks",
    "heavy_trucks": "Heavy trucks", "school_buses": "School buses",
    "urban_transit": "Transit buses", "inter_city_buses": "Intercity buses",
    "motorcycles": "Motorcycles",
}
FIGURES = {
    "vehicle-cost-1": "Source-class investment trajectories retain their price basis",
    "vehicle-cost-2": "Emitted investment costs combine class and range choices",
    "vehicle-cost-3": "Empirical age relationships determine maintenance and repair costs",
    "vehicle-cost-4": "Service-normalized maintenance costs depend on age and load",
    "vehicle-cost-5": "A representative range cost preserves the specified range mix",
}
POWERTRAIN_COLORS = {
    "GASOLINE": "#8c564b", "DIESEL": "#4e79a7", "CNG": "#b07aa1", "HEV": "#59a14f",
    "PHEV 35 MI": "#f28e2b", "PHEV 50 MI": "#e15759", "BEV 150 MI": "#76b7b2",
    "BEV 200 MI": "#499894", "BEV 300 MI": "#9d7660", "BEV 400 MI": "#d37295",
    "FCEV": "#79706e", "FCHEV": "#b6992d", "BEV": "#499894", "PHEV": "#f28e2b",
    "BEV REPRESENTATIVE": "#111111", "PHEV REPRESENTATIVE": "#555555",
    "CONV": "#4e79a7", "PARHEV": "#59a14f", "SERIESPHEV": "#f28e2b", "ISG": "#86bcb6",
}


@dataclass(frozen=True)
class VehicleCostDiagnostic:
    frames: dict[str, pd.DataFrame]
    metadata: dict
    directory: Path | None = None


def _metadata(bundle: ConfigBundle, source: str, component: str) -> tuple[str, int]:
    adapter = bundle.sources.sources[source].component(component).adapter
    return str(adapter["native_cost_currency"]), int(adapter["native_cost_dollar_year"])


def source_cost_frames(
    prices: pd.DataFrame, *, burnham: dict[str, pd.DataFrame], bean: pd.DataFrame,
    converter: CerCurrencyConverter, rules: dict, ages: list[int],
    burnham_money: tuple[str, int], bean_money: tuple[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retain native money and a common CAD basis without altering empirical inputs."""
    if not ages or any(not isinstance(age, int) or age < 0 for age in ages):
        raise ValueError("Diagnostic ages must be explicit nonnegative integer years")
    investment = prices.copy()
    investment["retail_native"] = investment.vehicle_price_usd_2022_per_vehicle
    investment["manufacturing_native"] = investment.manufacturing_cost_usd_2022_per_vehicle
    for basis in ("retail", "manufacturing"):
        investment[f"{basis}_cad"] = [
            converter.convert(float(value), currency=str(currency), dollar_year=int(year)).target_cad_value
            for value, currency, year in zip(investment[f"{basis}_native"], investment.source_currency,
                                             investment.source_dollar_year)
        ]
    investment["target_dollar_year"] = converter.target_year
    investment["source_support"] = "Source projection point; no extrapolation"
    records = []
    for row in investment.loc[investment.family.eq("ldv")].itertuples(index=False):
        for age in ages:
            result = ldv_repair_per_mile(
                age=age, msrp=row.retail_native, vehicle_class=row.vehicle_class,
                powertrain=row.powertrain, **burnham,
                powertrain_labels=rules["ldv_powertrain_evidence"],
                price_exponent=rules["ldv_repair_price_exponent_per_usd"],
                max_age=rules["ldv_repair_max_age"],
            )
            for component in ("repair", "maintenance", "total"):
                records.append({
                    "family": "ldv", "vehicle_class": row.vehicle_class, "powertrain": row.powertrain,
                    "source_year": row.source_year, "age": age, "component": component,
                    "cost_native": result[component], "source_currency": burnham_money[0],
                    "source_dollar_year": burnham_money[1], "retail_msrp": row.retail_native,
                    "msrp_currency": row.source_currency, "msrp_dollar_year": row.source_dollar_year,
                    "source_support": ("Repair held at terminal coefficient age" if age > rules["ldv_repair_max_age"]
                                       else "Transferred US empirical age coefficients"),
                })
    # MHDV coefficients do not have a vehicle-price year dependency.
    for vehicle_class, powertrain in bean[["vehicle_class", "vehicle_powertrain"]].drop_duplicates().itertuples(index=False):
        for age in ages:
            result = bean_maintenance_per_mile(bean, vehicle_class=vehicle_class, powertrain=powertrain, age=age)
            records.append({
                "family": "mhdv", "vehicle_class": vehicle_class, "powertrain": powertrain,
                "source_year": None, "age": age, "component": "total",
                "cost_native": result["source_cost_per_mile"], "source_currency": bean_money[0],
                "source_dollar_year": bean_money[1], "retail_msrp": None,
                "msrp_currency": None, "msrp_dollar_year": None,
                "source_support": "Linear empirical model; validated age domain is not registered",
            })
    maintenance = pd.DataFrame(records)
    maintenance["cost_cad"] = [
        converter.convert(float(value), currency=str(currency), dollar_year=int(year)).target_cad_value
        for value, currency, year in zip(maintenance.cost_native, maintenance.source_currency, maintenance.source_dollar_year)
    ]
    maintenance["target_dollar_year"] = converter.target_year
    maintenance["denominator"] = "vehicle-mile"
    return investment, maintenance


def range_cost_comparison(investment: pd.DataFrame, shares: pd.DataFrame, category_to_market: dict) -> pd.DataFrame:
    """Illustrate the supported linear cost mix; never claim a counterfactual was emitted."""
    output = []
    selected = investment.loc[investment.powertrain.str.startswith(("bev_", "phev_"))].copy()
    selected["powertrain_family"] = selected.powertrain.str.split("_").str[0].str.upper()
    for (region, mode, vintage, family), rows in selected.groupby(["region", "mode", "vintage", "powertrain_family"]):
        market = category_to_market[mode]
        weights = shares.loc[shares.market_class.eq(market) & shares.powertrain.eq(family)].set_index("bucket").share_of_all_sales
        if rows.powertrain.duplicated().any() or set(rows.powertrain) != set(weights.index):
            raise ValueError("Range cost comparison needs the complete, unique family")
        if rows.units.nunique() != 1 or not np.isclose(weights.sum(), 1, atol=1e-12, rtol=0):
            raise ValueError("Range cost comparison requires matching units and complete shares")
        rows = rows.assign(weight=rows.powertrain.map(weights), representation="Individual range")
        output.extend(rows.to_dict("records"))
        output.append({
            "region": region, "mode": mode, "vintage": vintage, "powertrain_family": family,
            "powertrain": f"{family.lower()}_representative", "cost": float((rows.cost * rows.weight).sum()),
            "units": rows.units.iloc[0], "purchase_evidence_year": rows.purchase_evidence_year.iloc[0],
            "weight": 1.0, "representation": "OMEGA weighted illustration; not an additional emitted row",
        })
    return pd.DataFrame(output)


def source_step_audit(prices: pd.DataFrame) -> pd.DataFrame:
    """Expose every adjacent source change; a large change alone is not a defect."""
    keys = ["family", "vehicle_class", "powertrain"]
    result = prices.sort_values([*keys, "source_year"]).copy()
    groups = result.groupby(keys, sort=False)
    result["previous_source_year"] = groups.source_year.shift()
    result["previous_native_price"] = groups.retail_native.shift()
    result["relative_change"] = result.retail_native / result.previous_native_price - 1
    return result.loc[result.previous_source_year.notna()].sort_values("relative_change", key=abs, ascending=False)


def reconcile_emitted_costs(audit: pd.DataFrame, emitted: pd.DataFrame, *, variable: bool = False) -> int:
    """Check shared source-audit/SQLite rows without equating pre/post range scopes."""
    keys = ["region", "tech", "vintage", *(["period"] if variable else [])]
    if audit.duplicated(keys).any() or emitted.duplicated(keys).any():
        raise ValueError("Duplicate vehicle cost audit or emitted keys")
    common = audit.merge(emitted, on=keys, suffixes=("_audit", "_emitted"), validate="one_to_one")
    if common.empty or not common.units_audit.eq(common.units_emitted).all() or not np.allclose(
        common.cost_audit, common.cost_emitted, rtol=1e-12, atol=1e-10,
    ):
        raise ValueError("Vehicle cost source audit disagrees with emitted SQLite values or units")
    absent = audit.merge(emitted[keys], on=keys, how="left", indicator=True)
    missing = absent.loc[absent._merge.eq("left_only")]
    # Only new LDV range rows can disappear in the subsequent representative step.
    if not missing.empty and not (
        missing["mode"].isin(["cars", "passenger_light_trucks", "freight_light_trucks"])
        & missing.tech.str.endswith("_N") & missing.tech.str.contains(r"(?:BEV|PHEV)\d")
    ).all():
        raise ValueError("Vehicle cost source audit has unexplained missing emitted rows")
    return len(common)


def _read_sqlite(database: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        technology = pd.read_sql_query("SELECT tech,category,sub_category FROM technology", connection)
        frames = [pd.read_sql_query(f"SELECT * FROM {table}", connection)
                  for table in ("cost_invest", "cost_variable")]
    technology = technology.rename(columns={"category": "mode", "sub_category": "powertrain"})
    technology = technology.loc[technology["mode"].isin(ROAD_MODES)]
    return technology, *(frame.merge(technology, on="tech", validate="many_to_one") for frame in frames)


def _input_paths(bundle: ConfigBundle) -> dict[str, Path]:
    atb = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    directory = resolve_input_path(bundle, "interim", atb["interim_subdir"])
    cer = load_harmonization_rules(bundle, "cer_enerfuture")
    edition = bundle.scenario.sources.selections["cer_canadas_energy_future"].edition
    costs = load_harmonization_rules(bundle, "costs")
    ranges = module_rules(bundle)
    return {
        "scenario": bundle.scenario_path, "paths": bundle.paths_path, "sources": bundle.sources_path,
        "rules": resolve_parameter_path(bundle, "rules.yaml"),
        "conversion": resolve_parameter_path(bundle, "conversion.yaml"),
        "vehicles": directory / atb["components"]["vehicles"]["output_file"],
        "bean": directory / atb["components"]["anl_bean"]["output_file"],
        **{name: directory / filename for name, filename in atb["components"]["maintenance_ldv"]["outputs"].items()},
        "macro": resolve_input_path(bundle, "interim", cer["interim_subdir_template"].format(edition=edition),
                                    cer["components"]["macro-indicators"]["output_file"]),
        "road_invest": resolve_artifact_path(bundle, "costs_interim", costs["files"]["road_invest"]),
        "road_variable": resolve_artifact_path(bundle, "costs_interim", costs["files"]["road_variable"]),
        "cost_integrity": resolve_artifact_path(bundle, "costs_validation", costs["files"]["integrity"]),
        "range_records": resolve_artifact_path(bundle, "ldv_range_validation", ranges.files["records"]),
        "template_technology": resolve_input_path(bundle, "template", "technology.csv"),
    }


def build_diagnostic(bundle: ConfigBundle) -> VehicleCostDiagnostic:
    """Read verified prepared evidence and calculate diagnostics without writes or ETL."""
    database = resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    report_path = resolve_repo_path(bundle.repo_root, bundle.scenario.outputs.validation_report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    verify_build_snapshot(bundle, database, report)
    paths = _input_paths(bundle)
    paths.update(database=database, build_report=report_path)
    hashes = {key: file_sha256(path) for key, path in paths.items()}
    audit = json.loads(paths["cost_integrity"].read_text(encoding="utf-8"))
    if audit["input_digest"] != report["costs"]["input_digest"]:
        raise ValueError("Vehicle cost audit belongs to a different build; rebuild the selected scenario")
    frames = {key: pd.read_csv(path) for key, path in paths.items() if path.suffix == ".csv"}
    cost_rules = load_harmonization_rules(bundle, "costs")
    trajectory = configured_trajectory(bundle, parameter="costs")
    source_money = _metadata(bundle, "nlr_atb_transportation", "vehicles")
    prices = select_nlr_purchase_prices(
        frames["vehicles"], trajectory=trajectory,
        efficiency_rules=load_harmonization_rules(bundle, "efficiencies"),
        rpe_markups=cost_rules["nlr_road_retail_price_equivalent_markup"],
        source_currency=source_money[0], source_dollar_year=source_money[1],
    )
    converter = CerCurrencyConverter(frames["macro"], scenario=bundle.scenario.economics.cer_scenario,
                                     target_year=bundle.scenario.economics.cost_reference_year)
    ages = list(range(int(frames["road_variable"].age.dropna().max()) + 1))
    source_invest, source_maintenance = source_cost_frames(
        prices, burnham={"baseline": frames["baseline_repair_cost"], "class_multipliers": frames["class_multipliers"],
                         "powertrain_multipliers": frames["powertrain_multipliers"], "maintenance": frames["maintenance_cost"]},
        bean=frames["bean"], converter=converter, rules=cost_rules, ages=ages,
        burnham_money=_metadata(bundle, "nlr_atb_transportation", "maintenance_ldv"),
        bean_money=_metadata(bundle, "anl_autonomie_bean", "mhdv_maintenance_coefficients"),
    )
    technology, invest, variable = _read_sqlite(database)
    reconciled = {
        "investment_shared_rows": reconcile_emitted_costs(frames["road_invest"], invest),
        "maintenance_shared_rows": reconcile_emitted_costs(frames["road_variable"], variable, variable=True),
    }
    invest = invest.assign(purchase_evidence_year=invest.vintage.map(
        lambda year: bundle.scenario.periods.projection_year(int(year), legacy_at_end=False)))
    audit_keys = ["region", "tech", "vintage", "period"]
    details = frames["road_variable"][[*audit_keys, "age", "load_factor", "purchase_evidence_year", "treatment"]]
    variable = variable.merge(details, on=audit_keys, how="left", validate="one_to_one")
    # Representatives are emitted after source audits; their years still use the
    # same scenario clock. Do not invent source-class attribution for those rows.
    missing = variable.age.isna() & variable.tech.str.endswith("_N")
    variable.loc[missing, "age"] = [
        bundle.scenario.periods.projection_year(int(period), legacy_at_end=False)
        - bundle.scenario.periods.projection_year(int(vintage), legacy_at_end=False)
        for period, vintage in zip(variable.loc[missing, "period"], variable.loc[missing, "vintage"])
    ]
    template = frames["template_technology"].rename(columns={"category": "mode", "sub_category": "powertrain"})
    pre_range = frames["road_invest"].merge(template[["tech", "powertrain"]], on="tech", validate="many_to_one")
    range_rules = module_rules(bundle)
    range_evidence = bucket_range_evidence(frames["range_records"], rules=range_rules)
    range_evidence.provenance_contexts.append(resolve_provenance(
        bundle.sources, source_key="epa_omega_baseline", component_key="baseline_sales_ranges",
        transformation="Recorded vehicle-cost range comparison", transformation_version="1",
        value_variant={"recorded_range_sha256": hashes["range_records"], "rules": range_rules.model_dump()},
    ))
    validate_share_evidence(range_evidence, rules=range_rules)
    outputs = {
        "source_investment": source_invest, "source_maintenance": source_maintenance,
        "investment": invest, "maintenance": variable, "technology": technology,
        "investment_audit": pre_range, "maintenance_audit": frames["road_variable"],
        "source_steps": source_step_audit(source_invest), "range_shares": range_evidence.bucket_totals,
        "range_comparison": range_cost_comparison(pre_range, range_evidence.bucket_totals, range_rules.category_to_market),
    }
    if any(file_sha256(path) != hashes[key] for key, path in paths.items()):
        raise ValueError("Vehicle cost inputs changed while capturing evidence; rerun serially")
    return VehicleCostDiagnostic(outputs, {
        "version": 1, "scenario": bundle.scenario.model_dump(mode="json"),
        "scenario_path": str(bundle.scenario_path.relative_to(bundle.repo_root)),
        "build_timestamp_utc": report["timestamp_utc"], "database_sha256": report["database_sha256"],
        "cost_input_digest": audit["input_digest"], "trajectory": trajectory,
        "emitted_reconciliation": reconciled,
        "inputs": {key: {"source": str(path.relative_to(bundle.repo_root)), "source_sha256": hashes[key]}
                   for key, path in paths.items()},
        "age_scope": "Integer ages from zero through the maximum age in this build's road M&R audit",
        "ages": ages, "repair_terminal_age": cost_rules["ldv_repair_max_age"],
        "figures": FIGURES, "cost_metadata": audit["source_cost_metadata"],
        "limitations": [
            "ATB retail MSRP uses a different dollar year from the calibrated LDV repair coefficients; production preserves that mismatch.",
            "US empirical M&R coefficients are transferred to Canadian classes; agreement with parameter rows is implementation reconciliation, not independent validation.",
            "The age range is a diagnostic view of model use, not evidence of empirical validation at those ages.",
            "Source-class charts retain actual source years; emitted vintage labels and interval-end cost evidence years are different.",
            "Representative range illustrations apply the existing OMEGA shares and are not an additional scenario build or optimizer result.",
            "Source-class curves cover ATB LDV/MHDV evidence and BEAN coefficients; buses/motorcycles also appear in emitted-row views with their source treatments.",
        ],
    })


def write_diagnostic(diagnostic: VehicleCostDiagnostic, bundle: ConfigBundle) -> Path:
    """Publish immutable local evidence, followed by the latest-successful pointer."""
    root = resolve_artifact_path(bundle, ROUTE)
    implementation = [Path(__file__), bundle.repo_root / "src/parameterization/road_capex_opex.py",
                      bundle.repo_root / "src/parameterization/currency.py"]
    metadata = {**deepcopy(diagnostic.metadata), "implementation": {
        str(path.relative_to(bundle.repo_root)): file_sha256(path) for path in implementation}}
    identity = hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest()[:16]
    capture_id = f"{bundle.scenario.scenario.name}-{identity}"
    release = root / "captures" / capture_id
    if release.exists():
        load_diagnostic(directory=release)
        write_text_atomic(json.dumps({"capture": f"captures/{capture_id}"}, indent=2), root / "latest.json")
        return release
    directory = root / f".staging-{uuid4().hex}"
    directory.mkdir(parents=True)
    try:
        _write_capture(diagnostic, metadata, directory, bundle, capture_id)
        load_diagnostic(directory=directory)
        release.parent.mkdir(parents=True, exist_ok=True)
        directory.rename(release)
        write_text_atomic(json.dumps({"capture": f"captures/{capture_id}"}, indent=2), root / "latest.json")
    finally:
        if directory.exists():
            if not directory.resolve().is_relative_to(root.resolve()) or not directory.name.startswith(".staging-"):
                raise ValueError("Vehicle cost cleanup target is outside the diagnostic staging root")
            shutil.rmtree(directory)
    return release


def _write_capture(diagnostic: VehicleCostDiagnostic, metadata: dict, directory: Path,
                   bundle: ConfigBundle, capture_id: str) -> None:
    """Write into private staging; publication occurs only after hash verification."""
    metadata.update(capture_id=capture_id, captured_at=datetime.now(UTC).isoformat(), frames={})
    for name, frame in diagnostic.frames.items():
        path = directory / f"{name}.csv"
        write_dataframe_atomic(frame, path)
        metadata["frames"][name] = {"path": path.name, "sha256": file_sha256(path), "rows": len(frame)}
    for name, record in metadata["inputs"].items():
        # The large database is identified by hash; its selected output rows are
        # stored above. Preserve all source/config/audit bytes used in derivation.
        if name == "database":
            continue
        source = resolve_repo_path(bundle.repo_root, record["source"])
        target = directory / "inputs" / f"{name}{source.suffix}"
        target.parent.mkdir(exist_ok=True)
        if file_sha256(source) != record["source_sha256"]:
            raise ValueError(f"Vehicle cost source changed before publication: {source}")
        shutil.copyfile(source, target)
        if file_sha256(target) != record["source_sha256"]:
            raise ValueError(f"Vehicle cost snapshot changed during copying: {source}")
        record["path"] = target.relative_to(directory).as_posix()
    write_text_atomic(json.dumps(metadata, indent=2, sort_keys=True), directory / "manifest.json")


def _within(directory: Path, relative: str) -> Path:
    path = (directory / relative).resolve()
    if not path.is_relative_to(directory.resolve()) or path == directory.resolve():
        raise ValueError("Vehicle cost evidence path escapes its capture")
    return path


def load_diagnostic(*, directory: Path | None = None, repo_root: Path | None = None) -> VehicleCostDiagnostic:
    """Load only immutable captured bytes; never select a scenario or run preparation."""
    if directory is None:
        root = (repo_root or find_repo_root()).resolve()
        paths = PathsConfig.model_validate(load_yaml(root / "config/paths.yaml"))
        evidence_root = resolve_repo_path(root, paths.artifacts[ROUTE].path)
        index = evidence_root / "latest.json"
        if not index.is_file():
            raise FileNotFoundError("Vehicle-cost evidence is missing. Prepare the selected scenario, then run "
                                    "uv run python -m validation.vehicle_cost_diagnostic --scenario <scenario.yaml> --write-report")
        directory = _within(evidence_root, json.loads(index.read_text(encoding="utf-8"))["capture"])
    metadata = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    for record in metadata["inputs"].values():
        if "path" in record and file_sha256(_within(directory, record["path"])) != record["source_sha256"]:
            raise ValueError("Vehicle cost captured input hash changed; regenerate the diagnostic")
    frames = {}
    for name, record in metadata["frames"].items():
        path = _within(directory, record["path"])
        if file_sha256(path) != record["sha256"]:
            raise ValueError(f"Vehicle cost captured frame hash changed: {name}")
        frames[name] = pd.read_csv(path)
        if len(frames[name]) != record["rows"]:
            raise ValueError(f"Vehicle cost captured row count changed: {name}")
    return VehicleCostDiagnostic(frames, metadata, directory)


def _chart(frame: pd.DataFrame, *, figure: str, x: str, y: str, x_title: str, y_title: str, subtitle: list[str]):
    import altair as alt

    if frame.empty:
        raise ValueError(f"No supported rows for {figure} selection")
    if not np.isfinite(frame[[x, y]].to_numpy(dtype=float)).all():
        raise ValueError("Cost charts require finite plotted values")
    plot = frame.assign(series=frame.powertrain.str.replace("_miles", " mi", regex=False).str.replace("_", " ", regex=False).str.upper())
    fields = [x, y, "series"]
    for field in ("tech", "vintage", "source_year", "purchase_evidence_year", "representation"):
        if field in plot and field not in fields:
            fields.append(field)
    data = alt.Data(values=json.loads(plot[fields].to_json(orient="records")))
    domain = list(POWERTRAIN_COLORS)
    if unknown := set(plot.series) - set(domain):
        raise ValueError(f"Cost plot requires explicit stable powertrain colours: {sorted(unknown)}")
    chart = alt.Chart(data).mark_line(point=True, strokeWidth=2).encode(
        x=alt.X(f"{x}:Q", title=x_title, axis=alt.Axis(format="d", tickMinStep=1)),
        y=alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(zero=True)),
        color=alt.Color("series:N", title="Powertrain / range", scale=alt.Scale(domain=domain, range=list(POWERTRAIN_COLORS.values())),
                        legend=alt.Legend(orient="right", labelLimit=280, values=sorted(plot.series.unique()))),
        detail="series:N", order=f"{x}:Q", tooltip=[alt.Tooltip(f"{x}:Q"), alt.Tooltip(f"{y}:Q", format=",.3f"), "series:N"],
    ).properties(width=700, height=320)
    return identified_chart(chart, figure, FIGURES[figure]).properties(
        title=alt.Title(FIGURES[figure], subtitle=[f"Figure {figure}",
            *(line for context in subtitle for line in textwrap.wrap(context, width=105))], anchor="start"))


def source_price_chart(diagnostic: VehicleCostDiagnostic, *, vehicle_class: str = "Compact", basis: str = "manufacturing", currency: str = "CAD"):
    if basis not in {"retail", "manufacturing"} or currency not in {"CAD", "native"}:
        raise ValueError("Choose retail/manufacturing and CAD/native")
    frame = diagnostic.frames["source_investment"].loc[lambda data: data.vehicle_class.eq(vehicle_class)]
    unit = f"{int(frame.target_dollar_year.iloc[0])} CAD/vehicle" if currency == "CAD" else f"{int(frame.source_dollar_year.iloc[0])} {frame.source_currency.iloc[0]}/vehicle"
    return _chart(frame, figure="vehicle-cost-1", x="source_year", y=f"{basis}_{'cad' if currency == 'CAD' else 'native'}",
                  x_title="Source projection year", y_title=f"{basis.title()} cost ({unit})",
                  subtitle=[f"{vehicle_class} · {diagnostic.metadata['trajectory']} · source points, no extrapolation",
                            "Manufacturing estimates reverse the family RPE markup; retail MSRP remains the repair input."])


def investment_trajectory_chart(diagnostic: VehicleCostDiagnostic, *, region: str = "ON", mode: str = "cars"):
    frame = diagnostic.frames["investment"].loc[lambda data: data.region.eq(region) & data["mode"].eq(mode)]
    return _chart(frame, figure="vehicle-cost-2", x="purchase_evidence_year", y="cost", x_title="Purchase evidence calendar year",
                  y_title=str(frame.units.iloc[0]), subtitle=[f"{region} · {ROAD_MODES[mode]} · {diagnostic.metadata['trajectory']}",
                  f"Range mode: {diagnostic.metadata['scenario']['BEV_PHEV_range_representation']['mode']} · emitted SQLite rows",
                  "The x-axis uses the cost evidence year; vintage labels remain in the audit table."])


def maintenance_age_chart(diagnostic: VehicleCostDiagnostic, *, family: str = "ldv", vehicle_class: str = "Compact", source_year: int = 2025, currency: str = "CAD"):
    if currency not in {"CAD", "native"}:
        raise ValueError("Choose CAD/native")
    frame = diagnostic.frames["source_maintenance"].loc[lambda data: data.family.eq(family) & data.vehicle_class.eq(vehicle_class) & data.component.eq("total")]
    if family == "ldv":
        frame = frame.loc[frame.source_year.eq(source_year)]
    money = f"{int(frame.target_dollar_year.iloc[0])} CAD" if currency == "CAD" else f"{int(frame.source_dollar_year.iloc[0])} {frame.source_currency.iloc[0]}"
    return _chart(frame, figure="vehicle-cost-3", x="age", y="cost_cad" if currency == "CAD" else "cost_native",
                  x_title="Vehicle age (years)", y_title=f"M&R ({money}/vehicle-mile)",
                  subtitle=[f"{vehicle_class} · {f'MSRP source year {source_year}' if family == 'ldv' else 'BEAN coefficients held across vintages'}",
                  (f"Repair holds after age {diagnostic.metadata['repair_terminal_age']}; US coefficients are transferred to Canadian vehicles."
                   if family == "ldv" else "Age-linear MHDV coefficients have no registered validated age domain."),
                  ("Retail MSRP and repair coefficients retain their different dollar years."
                   if family == "ldv" else "No MSRP factor is applied. Legend labels are the registered BEAN powertrain codes.")])


def service_maintenance_chart(diagnostic: VehicleCostDiagnostic, *, region: str = "ON", mode: str = "cars", vintage: int = 2025):
    frame = diagnostic.frames["maintenance"].loc[lambda data: data.region.eq(region) & data["mode"].eq(mode) & data.vintage.eq(vintage) & data.tech.str.endswith("_N")]
    age_based = frame.age.notna().all()
    return _chart(frame, figure="vehicle-cost-4", x="age" if age_based else "period", y="cost",
                  x_title="Vehicle age at the cost period (years)" if age_based else "Model period label (annual source costs; no empirical age relationship)",
                  y_title=str(frame.units.iloc[0]), subtitle=[f"{region} · {ROAD_MODES[mode]} · new vintage {vintage}",
                  "Emitted M&R excludes fuel purchases; service denominators use the captured occupancy/payload assumptions.",
                  "Only active process-period rows are shown; an absent later age is not zero cost."])


def range_cost_chart(diagnostic: VehicleCostDiagnostic, *, region: str = "ON", mode: str = "cars", family: str = "BEV"):
    frame = diagnostic.frames["range_comparison"].loc[lambda data: data.region.eq(region) & data["mode"].eq(mode) & data.powertrain_family.eq(family)]
    return _chart(frame, figure="vehicle-cost-5", x="purchase_evidence_year", y="cost", x_title="Purchase evidence calendar year",
                  y_title=str(frame.units.iloc[0]), subtitle=[f"{region} · {ROAD_MODES[mode]} · {family} · same class weights and currency basis",
                  "Representative line: existing OMEGA sales-weighted cost arithmetic; diagnostic illustration, not a separate build.",
                  "This fixes within-family composition; it neither fixes BEV/PHEV adoption nor proves equivalence of complete scenarios."])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--write-report", action="store_true", help="Explicitly publish the diagnostic evidence capture")
    args = parser.parse_args()
    bundle = load_config_bundle(args.scenario)
    diagnostic = build_diagnostic(bundle)
    if args.write_report:
        print(write_diagnostic(diagnostic, bundle))
    else:
        print(json.dumps({"scenario": diagnostic.metadata["scenario"]["scenario"]["name"],
                          "frames": {key: len(frame) for key, frame in diagnostic.frames.items()}}, indent=2))


if __name__ == "__main__":
    main()
