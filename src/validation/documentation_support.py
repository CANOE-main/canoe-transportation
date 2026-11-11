"""Recorded presentation evidence for separate Ontario fleet composition marginals.

No joint size/model/powertrain distribution is inferred. MTO supplies mapped
FIT_ACTIVE size/model evidence; StatCan supplies a distinct new-registration fuel
marginal. Reading the notebook never loads mutable interim inputs or runs ETL.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
import textwrap

import altair as alt
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from utils import (
    file_sha256, find_repo_root, load_config_bundle, load_harmonization_rules, load_yaml,
    resolve_configured_path, resolve_repo_path, write_dataframe_atomic, write_text_atomic,
)
from validation.config_models import PathsConfig
from validation.ldv_composition_benchmark import _statcan
from validation.notebook_evidence import load_notebook_evidence
from validation.notebook_figures import identified_chart
from validation.sqlite_utils import open_sqlite_readonly


class FleetPresentation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    reference_year: int
    region: str
    mto_report_year: int
    statcan_maximum_year: int
    icons_per_panel: int = Field(gt=0)
    top_models_per_vehicle_group: int = Field(gt=0)
    vehicle_groups: list[str]
    powertrains: list[str]
    size_class_order: dict[str, list[str]]
    figure_id: str
    exact_table: str
    panel_columns: int = Field(gt=0)
    panel_width: int = Field(gt=0)
    panel_height: int = Field(gt=0)


class NonroadPresentation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    scenario: str
    region: str
    modes: dict[str, str]
    figure_id: str
    exact_table: str
    panel_width: int = Field(gt=0)
    panel_height: int = Field(gt=0)


@dataclass(frozen=True)
class FleetPresentationEvidence:
    shares: pd.DataFrame
    metadata: dict
    settings: FleetPresentation


@dataclass(frozen=True)
class NonroadPresentationEvidence:
    rows: pd.DataFrame
    metadata: dict
    settings: NonroadPresentation


def largest_remainder(counts: pd.Series, total_icons: int) -> pd.Series:
    """Allocate integer symbols while preserving exact shares separately, including zero icons."""
    values = pd.to_numeric(counts, errors="raise").astype(float)
    if values.empty or not np.isfinite(values).all() or (values < 0).any() or values.sum() <= 0:
        raise ValueError("Fleet shares require nonnegative finite counts and a positive total")
    quotas = values / values.sum() * total_icons
    result = np.floor(quotas).astype(int)
    remainder = total_icons - int(result.sum())
    ordering = pd.DataFrame({"remainder": quotas - result, "label": counts.index.astype(str)}, index=counts.index)
    winners = ordering.sort_values(["remainder", "label"], ascending=[False, True], kind="stable").index[:remainder]
    result.loc[winners] += 1
    if result.sum() != total_icons:
        raise ValueError("Fleet icon allocation did not preserve its panel denominator")
    return result


def _panel(counts: pd.Series, *, group: str, dimension: str, year: int, source: str,
           denominator_label: str, settings: FleetPresentation) -> pd.DataFrame:
    counts = counts.astype(float)
    if (counts % 1 != 0).any():
        raise ValueError("Fleet count evidence must contain integer vehicles")
    counts = counts.astype(int)
    symbols = largest_remainder(counts, settings.icons_per_panel)
    return pd.DataFrame({
        "vehicle_group": group, "dimension": dimension, "category": counts.index,
        "count": counts.to_numpy(), "denominator": int(counts.sum()),
        "share": (counts / counts.sum()).to_numpy(), "icons": symbols.to_numpy(),
        "source_year": year, "reference_year": settings.reference_year,
        "source": source, "denominator_label": denominator_label,
        "carried_forward_years": settings.reference_year - year,
    })


def derive_fleet_shares(mapped: pd.DataFrame, statcan: pd.DataFrame, settings: FleetPresentation) -> pd.DataFrame:
    """Derive independent marginals with explicit mapping and observation denominators."""
    if settings.region != "ON" or set(settings.vehicle_groups) != {"Cars", "Light trucks"}:
        raise ValueError("This presentation requires Ontario and separate Cars/Light trucks marginals")
    required = {"report_year", "FIT_ACTIVE", "mapping_accepted", "mapping_outcome", "nrcan_ceud_class",
                "nrcan_vehicle_class", "canonical_make", "canonical_model", "VEHICLE_CLASS", "MAKE", "MODEL", "MODEL_YEAR"}
    if missing := required - set(mapped):
        raise ValueError(f"Captured mapped MTO evidence lacks {sorted(missing)}")
    fleet = mapped.loc[mapped.report_year.eq(settings.mto_report_year)].copy()
    if fleet.empty or fleet.duplicated(["report_year", "VEHICLE_CLASS", "MAKE", "MODEL", "MODEL_YEAR"]).any():
        raise ValueError("MTO report-year coverage is missing or duplicated")
    fleet["FIT_ACTIVE"] = pd.to_numeric(fleet.FIT_ACTIVE, errors="raise")
    if fleet.FIT_ACTIVE.isna().any() or not np.isfinite(fleet.FIT_ACTIVE).all() or (fleet.FIT_ACTIVE < 0).any():
        raise ValueError("MTO FIT_ACTIVE counts must be finite and nonnegative")
    if not fleet.mapping_accepted.isin([True, False]).all():
        raise ValueError("MTO class acceptance must be an explicit Boolean")
    accepted = fleet.mapping_accepted & fleet.nrcan_ceud_class.isin(["Car", "Light Truck"])
    fleet["group"] = fleet.nrcan_ceud_class.map({"Car": "Cars", "Light Truck": "Light trucks"}).where(accepted)
    output = []
    for group in settings.vehicle_groups:
        local = fleet.loc[fleet.group.eq(group)].copy()
        if local.empty:
            raise ValueError(f"Captured MTO evidence lacks accepted {group} coverage")
        labels = local.nrcan_vehicle_class.fillna("Unresolved size within mapped group")
        sizes = local.groupby(labels, sort=False).FIT_ACTIVE.sum()
        order = settings.size_class_order[group]
        sizes = sizes.reindex([name for name in order if name in sizes] + sorted(set(sizes.index) - set(order)))
        denominator = f"Mapped {group.lower()} FIT_ACTIVE vehicles in MTO report {settings.mto_report_year}"
        output.append(_panel(sizes, group=group, dimension="Size class", year=settings.mto_report_year,
                             source="MTO mapped FIT_ACTIVE / NRCan size classes", denominator_label=denominator, settings=settings))
        identified = local.canonical_make.notna() & local.canonical_model.notna()
        names = local.loc[identified].canonical_make.str.strip() + " " + local.loc[identified].canonical_model.str.strip()
        models = local.loc[identified].groupby(names, sort=True).FIT_ACTIVE.sum().sort_values(ascending=False, kind="stable")
        top = models.iloc[:settings.top_models_per_vehicle_group].copy()
        top.loc["Other / unidentified mapped models"] = int(local.FIT_ACTIVE.sum() - top.sum())
        output.append(_panel(top, group=group, dimension="Leading models", year=settings.mto_report_year,
                             source="MTO canonical make/model FIT_ACTIVE", denominator_label=denominator, settings=settings))
    if not statcan.source.eq("statcan_new").all():
        raise ValueError("LDV powertrain snapshots require StatCan new registrations, not registered stock")
    available = statcan.loc[statcan.region.eq(settings.region) & statcan.year.le(settings.statcan_maximum_year)]
    if available.empty:
        raise ValueError("No new-registration StatCan year supports the requested reference")
    year = int(available.year.max())
    if year > settings.reference_year or settings.mto_report_year > settings.reference_year:
        raise ValueError("Presentation cannot use observations later than its reference year")
    for group in settings.vehicle_groups:
        local = available.loc[available.year.eq(year) & available.vehicle_group.eq(group)]
        if local.status.eq("geography_proxy").any():
            raise ValueError("Ontario fleet illustration cannot silently substitute a geographic proxy")
        fuels = local.loc[local.powertrain.isin(settings.powertrains)]
        if fuels.powertrain.duplicated().any():
            raise ValueError("Duplicate StatCan fuel categories")
        if (set(fuels.powertrain) != set(settings.powertrains) or fuels["count"].isna().any()
                or not fuels.status.eq("reported").all()):
            raise ValueError(f"StatCan represented new-registration fuels are missing or suppressed for {group}/{year}")
        counts = fuels.set_index("powertrain")["count"].reindex(settings.powertrains)
        tables = sorted(set(fuels.source_table.dropna()))
        if not tables or set(tables) - {"20-10-0025-01", "20-10-0021-01"}:
            raise ValueError("LDV powertrain snapshot lacks registered new-registration source-table lineage")
        output.append(_panel(counts, group=group, dimension="Powertrain", year=year,
                             source=f"StatCan {' / '.join(tables)} new registrations",
                             denominator_label=f"Represented {group.lower()} new registrations in StatCan {year}; other fuels excluded",
                             settings=settings))
    result = pd.concat(output, ignore_index=True)
    result["panel"] = result.vehicle_group + " — " + result.dimension
    return result


def _controls(repo_root: Path | None = None) -> tuple[Path, PathsConfig, FleetPresentation, Path]:
    root = (repo_root or find_repo_root()).resolve()
    paths = PathsConfig.model_validate(load_yaml(root / "config/paths.yaml"))
    config = root / paths.config.parameters / "documentation.yaml"
    payload = load_yaml(config)
    if payload.get("version") != 1 or set(payload) - {"version", "fleet_100", "nonroad", "publication_figures", "charging_profile"}:
        raise ValueError("Unsupported documentation presentation configuration")
    return root, paths, FleetPresentation.model_validate(payload["fleet_100"]), config


def build_fleet_100(*, repo_root: Path | None = None) -> FleetPresentationEvidence:
    """Use only validated notebook captures and the explicit presentation controls."""
    root, _, settings, config = _controls(repo_root)
    stock = load_notebook_evidence("vehicle_stock", repo_root=root)
    efficiency = load_notebook_evidence("efficiency", repo_root=root)
    mapped = pd.read_csv(stock.paths["mapped_fleet"])
    native_registrations = pd.read_csv(efficiency.paths["benchmark_statcan_new"])
    spec = load_harmonization_rules(efficiency.bundle, "ldv_composition_benchmark")
    geography = load_harmonization_rules(efficiency.bundle, "statcan_tables")["geography"]
    annual = _statcan(native_registrations, spec, "statcan_new", [settings.region], geography)
    shares = derive_fleet_shares(mapped, annual, settings)
    selected_year = int(shares.loc[shares.dimension.eq("Powertrain"), "source_year"].iloc[0])
    excluded = annual.loc[annual.year.eq(selected_year) & ~annual.powertrain.isin(settings.powertrains),
                          ["vehicle_group", "year", "powertrain", "count", "status"]]
    metadata = {
        "figure_id": settings.figure_id, "reference_year": settings.reference_year,
        "stock_build": stock.metadata["build_id"], "efficiency_build": efficiency.metadata["build_id"],
        "stock_database": stock.metadata["database"], "efficiency_database": efficiency.metadata["database"],
        "settings_sha256": file_sha256(config), "settings_path": str(config.relative_to(root)),
        "generator": "validation.documentation_support.fleet_100_chart",
        "generator_sha256": file_sha256(Path(__file__)),
        "excluded_powertrain_counts": excluded.astype(object).where(excluded.notna(), None).to_dict("records"),
        "input_files": {
            "mapped_fleet": {"path": str(stock.paths["mapped_fleet"].relative_to(root)), "sha256": file_sha256(stock.paths["mapped_fleet"])},
            "benchmark_statcan_new": {"path": str(efficiency.paths["benchmark_statcan_new"].relative_to(root)), "sha256": file_sha256(efficiency.paths["benchmark_statcan_new"])},
        },
        "manifest_hashes": {
            "vehicle_stock": file_sha256(Path(stock.metadata["manifest"])),
            "efficiency": file_sha256(Path(efficiency.metadata["manifest"])),
        },
        "limitations": [
            "Each 100-symbol panel is a separate marginal with its own stated denominator; symbols cannot be matched across panels.",
            "MTO size/model shares condition on accepted mapped FIT_ACTIVE stock; unresolved classification is excluded from both Cars and Light trucks. Mapping coverage remains in the vehicle-population notebook.",
            "StatCan powertrain shares describe annual new registrations, not the surviving all-vintage MTO stock; no joint distribution is inferred.",
            "Powertrain panels normalize only represented fuels; other fuels are excluded, as in the production fuel-share calculation.",
            "An older StatCan observation is displayed as carry-forward to the reference year, not presented as a 2025 observation.",
            "Integer symbols use largest-remainder rounding; the exact table retains every category, including positive shares receiving zero symbols.",
        ],
    }
    return FleetPresentationEvidence(shares, metadata, settings)


def fleet_100_chart(evidence: FleetPresentationEvidence):
    """Six independent marginal views with exact shares and source denominators."""
    settings = evidence.settings
    colours = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#b07aa1", "#76b7b2", "#edc948", "#9c755f", "#bab0ab"]

    def panel(rows: pd.DataFrame):
        rows = rows.reset_index(drop=True).copy()
        labels = [str(value).replace("Sport utility vehicle:", "SUV:").replace("Station wagon:", "Wagon:")
                  .replace("Other / unidentified mapped models", "Other mapped models") for value in rows.category]
        labels = [{"gasoline": "Gasoline", "diesel": "Diesel", "bev": "BEV", "hev": "Hybrid", "phev": "Plug-in hybrid"}.get(value, value) for value in labels]
        palette = dict(zip(labels, colours, strict=False))
        symbols = []
        offset = 0
        for label, count in zip(labels, rows.icons):
            for index in range(offset, offset + int(count)):
                symbols.append({"column": index % settings.panel_columns, "row": index // settings.panel_columns, "category": label})
            offset += int(count)
        grid = alt.Chart(alt.Data(values=symbols)).mark_square(size=190, filled=True).encode(
            x=alt.X("column:O", axis=None), y=alt.Y("row:O", axis=None),
            color=alt.Color("category:N", scale=alt.Scale(domain=labels, range=[palette[label] for label in labels]), legend=None),
        ).properties(width=settings.panel_width, height=settings.panel_height)
        percentages = ["<0.01%" if 0 < value < 0.0001 else f"{value:.2%}" for value in rows.share]
        legends = [{"index": index, "label": f"{label}: {percentage} (n = {row.icons})", "category": label}
                   for index, (label, percentage, row) in enumerate(zip(labels, percentages, rows.itertuples(index=False)))]
        legend_base = alt.Chart(alt.Data(values=legends)).encode(y=alt.Y("index:O", axis=None))
        legend_keys = legend_base.mark_square(size=65, filled=True).encode(
            x=alt.value(4),
            color=alt.Color("category:N", scale=alt.Scale(domain=labels, range=[palette[label] for label in labels]), legend=None),
        )
        legend_text = legend_base.mark_text(align="left", fontSize=11, color="#222222").encode(x=alt.value(15), text="label:N")
        legend = alt.layer(legend_keys, legend_text).properties(width=settings.panel_width + 50, height=max(35, len(rows) * 17))
        first = rows.iloc[0]
        carry = f"; {first.source_year} carried to {first.reference_year}" if first.carried_forward_years else ""
        subtitle = [f"{first.source_year} · {int(first.denominator):,} vehicles{carry}",
                    *textwrap.wrap(first.denominator_label.replace("MTO", "Ontario MTO"), width=45)]
        group_title = first.vehicle_group.replace("All MTO records", "All Ontario MTO records")
        return alt.vconcat(grid, legend, spacing=7).properties(
            title=alt.Title(f"{group_title} · {first.dimension}", subtitle=subtitle, anchor="start", fontSize=13, subtitleFontSize=10))

    groups = []
    for group in settings.vehicle_groups:
        group_rows = evidence.shares.loc[evidence.shares.vehicle_group.eq(group)]
        groups.append(alt.hconcat(*(panel(group_rows.loc[group_rows.dimension.eq(dimension)])
                                  for dimension in ("Size class", "Leading models", "Powertrain")), spacing=8).resolve_scale(color="independent"))
    chart = alt.vconcat(*groups, spacing=30).resolve_scale(color="independent").configure_view(stroke=None)
    return identified_chart(chart, settings.figure_id, f"Ontario vehicles around {settings.reference_year}: separate views of 100").properties(
        title=alt.Title(f"Ontario vehicles around {settings.reference_year}: separate views of 100", anchor="start", fontSize=20, offset=35,
                        subtitle=[f"Figure {settings.figure_id} · Every panel has its own denominator; these are not the same 100 vehicles.",
                                  "Ontario MTO size/model marginals do not establish powertrain. StatCan shares are independent new-registration snapshots.",
                                  "n = # of symbols. Percentages are rounded to 0.01%; exact counts and shares remain in the table. Symbols use largest remainders."], subtitleFontSize=12))


def write_evidence(evidence: FleetPresentationEvidence, *, repo_root: Path | None = None) -> Path:
    root, paths, _, _ = _controls(repo_root)
    directory = resolve_repo_path(root, paths.artifacts["documentation_evidence"].path)
    table = directory / evidence.settings.exact_table
    write_dataframe_atomic(evidence.shares, table)
    metadata = {**evidence.metadata, "settings": evidence.settings.model_dump(),
                "table": {"path": table.name, "sha256": file_sha256(table)}}
    write_text_atomic(json.dumps(metadata, indent=2, sort_keys=True), directory / "fleet_100_manifest.json")
    return directory


def load_evidence(*, repo_root: Path | None = None) -> FleetPresentationEvidence:
    root, paths, settings, config = _controls(repo_root)
    directory = resolve_repo_path(root, paths.artifacts["documentation_evidence"].path)
    manifest = directory / "fleet_100_manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError("Fleet presentation evidence is missing. Run uv run python -m validation.documentation_support --write-report after refreshing the two notebook captures.")
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    if metadata["settings_sha256"] != file_sha256(config):
        raise ValueError("Fleet presentation selections changed; regenerate its recorded evidence")
    if metadata["generator_sha256"] != file_sha256(Path(__file__)):
        raise ValueError("Fleet presentation generator changed; regenerate its recorded evidence")
    for record in metadata["input_files"].values():
        if file_sha256(resolve_repo_path(root, record["path"])) != record["sha256"]:
            raise ValueError("Captured fleet presentation source changed")
    table = (directory / metadata["table"]["path"]).resolve()
    if not table.is_relative_to(directory.resolve()) or file_sha256(table) != metadata["table"]["sha256"]:
        raise ValueError("Fleet presentation exact table changed or escaped its evidence root")
    return FleetPresentationEvidence(pd.read_csv(table), metadata, settings)


def build_nonroad_evidence(*, repo_root: Path | None = None) -> NonroadPresentationEvidence:
    """Capture final parameters from a successful, hash-matched database; no raw inputs."""
    root, _, _, config = _controls(repo_root)
    settings = NonroadPresentation.model_validate(load_yaml(config)["nonroad"])
    bundle = load_config_bundle(resolve_repo_path(root, settings.scenario))
    database = resolve_configured_path(bundle, "outputs", "sqlite", bundle.scenario.outputs.sqlite_name)
    report_path = resolve_repo_path(root, bundle.scenario.outputs.validation_report)
    report_hash = file_sha256(report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    database_hash = file_sha256(database)
    if not report.get("ok") or Path(report["database"]).resolve() != database.resolve() or report["database_sha256"] != database_hash:
        raise ValueError("Non-road capture requires a successful report matching the actual database bytes")
    scenario_path = resolve_repo_path(root, settings.scenario)
    if report["config"]["scenario"]["sha256"] != file_sha256(scenario_path):
        raise ValueError("Non-road scenario changed after its recorded build")
    rules_record = next(item for item in report["config"]["parameters"] if Path(item["path"]).name == "rules.yaml")
    if file_sha256(Path(rules_record["path"])) != rules_record["sha256"]:
        raise ValueError("Non-road technology classification rules changed after the recorded build")
    existing_suffix = load_harmonization_rules(bundle, "efficiencies")["existing_suffix"]
    with closing(open_sqlite_readonly(database)) as connection:
        rows = pd.read_sql_query("SELECT e.*, t.category, t.sub_category FROM efficiency e JOIN technology t ON e.tech=t.tech", connection)
        capacity = pd.read_sql_query("SELECT c.region,c.tech,c.units,t.category FROM existing_capacity c JOIN technology t ON c.tech=t.tech", connection)
    rows = rows.loc[rows.region.eq(settings.region) & rows.category.isin(settings.modes)
                    & rows.vintage.isin(bundle.scenario.periods.model)
                    & ~rows.tech.str.endswith(existing_suffix)].copy()
    capacity = capacity.loc[capacity.region.eq(settings.region) & capacity.category.isin(settings.modes)]
    if file_sha256(database) != database_hash or file_sha256(report_path) != report_hash:
        raise ValueError("Non-road database changed while its final parameters were captured")
    validate_nonroad_rows(rows, settings)
    metadata = {
        "figure_id": settings.figure_id, "scenario": bundle.scenario.scenario.name,
        "build_timestamp_utc": report["timestamp_utc"],
        "database": {"path": str(database.relative_to(root)), "sha256": database_hash},
        "build_report": {"path": str(report_path.relative_to(root)), "sha256": report_hash},
        "recorded_build_config": report["config"], "periods": bundle.scenario.periods.model_dump(mode="json"),
        "scenario_selection": bundle.scenario.model_dump(mode="json"),
        "excluded_existing_suffix": existing_suffix,
        "capacity_units": {mode: sorted(capacity.loc[capacity.category.eq(mode), "units"].unique()) for mode in settings.modes},
        "settings_sha256": file_sha256(config), "generator_sha256": file_sha256(Path(__file__)),
        "generator": "validation.documentation_support.nonroad_efficiency_chart",
        "limitations": [
            "These are emitted new-vintage/model-period parameters, not measured consumption or independent model validation.",
            "Each panel retains its own service/energy unit. Passenger, freight and off-road energy-service values cannot be ranked on one common scale.",
            "The x-axis uses model-vintage labels, not source observation or projection years; captured scenario periods preserve the interpretation.",
            "Equal fuel trajectories can overlap. Fuel identity does not imply equal upstream emissions or total system costs.",
            "Off-road PJ/PJ is an energy-service accounting representation, not a measured thermodynamic efficiency or equipment stock.",
        ],
    }
    return NonroadPresentationEvidence(rows, metadata, settings)


def validate_nonroad_rows(rows: pd.DataFrame, settings: NonroadPresentation) -> None:
    if set(rows.category) != set(settings.modes) or not rows.region.eq(settings.region).all():
        raise ValueError("Non-road selected modes or region lack complete final parameter coverage")
    if rows.duplicated(["region", "tech", "vintage", "input_comm", "output_comm"]).any():
        raise ValueError("Non-road final parameter keys are duplicated")
    if not np.isfinite(rows.efficiency).all() or rows.efficiency.le(0).any():
        raise ValueError("Non-road service efficiency must be positive and finite")
    if rows.units.isna().any() or not rows.groupby("category").units.nunique().eq(1).all():
        raise ValueError("Non-road panels must retain one explicit unit per mode")


def nonroad_efficiency_chart(evidence: NonroadPresentationEvidence):
    """Separate mode panels preserve the emitted passenger, freight and energy units."""
    validate_nonroad_rows(evidence.rows, evidence.settings)
    panels = []
    fuels = sorted(evidence.rows.sub_category.unique())
    palette = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#b07aa1", "#76b7b2", "#9c755f", "#edc948", "#bab0ab"]
    for mode, label in evidence.settings.modes.items():
        rows = evidence.rows.loc[evidence.rows.category.eq(mode)].copy()
        rows["fuel"] = rows.sub_category.str.replace("_", " ", regex=False).str.upper()
        chart = alt.Chart(alt.Data(values=json.loads(rows[["vintage", "efficiency", "tech", "fuel"]].to_json(orient="records")))).mark_line(point=True).encode(
            x=alt.X("vintage:Q", title="Model vintage", axis=alt.Axis(format="d", tickMinStep=1, tickCount=5)),
            y=alt.Y("efficiency:Q", title=str(rows.units.iloc[0]), scale=alt.Scale(zero=True)),
            color=alt.Color("fuel:N", title="Input fuel", scale=alt.Scale(domain=[str(fuel).replace("_", " ").upper() for fuel in fuels], range=palette),
                            legend=alt.Legend(orient="bottom", direction="horizontal", values=sorted(rows.fuel.unique()), labelLimit=180)),
            strokeDash=alt.StrokeDash("fuel:N", legend=None), detail="tech:N", order="vintage:Q",
        ).properties(width=evidence.settings.panel_width, height=evidence.settings.panel_height,
                     title=alt.Title(label, anchor="start", subtitle=("Energy-service accounting; not a thermal efficiency" if mode == "off_road" else "Service output per PJ of vehicle input")))
        panels.append(chart)
    chart = alt.vconcat(*(alt.hconcat(*panels[index:index + 2], spacing=40).resolve_scale(y="independent", color="independent")
                          for index in range(0, len(panels), 2)), spacing=30).resolve_scale(y="independent", color="independent")
    title = "Non-road parameters use different service and energy denominators"
    return identified_chart(chart, evidence.settings.figure_id, title).properties(
        title=alt.Title(title, anchor="start", subtitle=[f"Figure {evidence.settings.figure_id} · {evidence.settings.region} · {evidence.metadata['scenario']} · emitted new-vintage parameters",
              f"Build {evidence.metadata['build_timestamp_utc']} · model-vintage labels; source years remain distinct.",
              "Panel scales and units differ. Equal fuel trajectories can overlap; these are model inputs, not observed performance."], subtitleFontSize=11))


def write_nonroad_evidence(evidence: NonroadPresentationEvidence, *, repo_root: Path | None = None) -> Path:
    root, paths, _, _ = _controls(repo_root)
    directory = resolve_repo_path(root, paths.artifacts["documentation_evidence"].path)
    table = directory / evidence.settings.exact_table
    write_dataframe_atomic(evidence.rows, table)
    metadata = {**evidence.metadata, "settings": evidence.settings.model_dump(), "table": {"path": table.name, "sha256": file_sha256(table)}}
    write_text_atomic(json.dumps(metadata, indent=2, sort_keys=True), directory / "nonroad_manifest.json")
    return directory


def load_nonroad_evidence(*, repo_root: Path | None = None) -> NonroadPresentationEvidence:
    root, paths, _, config = _controls(repo_root)
    settings = NonroadPresentation.model_validate(load_yaml(config)["nonroad"])
    directory = resolve_repo_path(root, paths.artifacts["documentation_evidence"].path)
    manifest = directory / "nonroad_manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError("Non-road presentation evidence is missing. Run uv run python -m validation.documentation_support --write-nonroad-report after its configured scenario build.")
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    if metadata["settings_sha256"] != file_sha256(config) or metadata["generator_sha256"] != file_sha256(Path(__file__)):
        raise ValueError("Non-road presentation selections or generator changed; regenerate its recorded evidence")
    table = (directory / metadata["table"]["path"]).resolve()
    if not table.is_relative_to(directory.resolve()) or file_sha256(table) != metadata["table"]["sha256"]:
        raise ValueError("Non-road presentation rows changed or escaped their evidence root")
    rows = pd.read_csv(table)
    validate_nonroad_rows(rows, settings)
    return NonroadPresentationEvidence(rows, metadata, settings)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-report", action="store_true")
    parser.add_argument("--write-nonroad-report", action="store_true")
    parser.add_argument("--export-svg", action="store_true", help="Explicit publication export from recorded evidence")
    parser.add_argument("--export-nonroad-svg", action="store_true")
    args = parser.parse_args()
    if args.write_report:
        print(write_evidence(build_fleet_100()))
    if args.write_nonroad_report:
        print(write_nonroad_evidence(build_nonroad_evidence()))
    if args.export_svg:
        evidence = load_evidence()
        root, paths, _, _ = _controls()
        path = resolve_repo_path(root, paths.artifacts["documentation_assets"].path) / f"{evidence.settings.figure_id}.svg"
        path.parent.mkdir(parents=True, exist_ok=True)
        fleet_100_chart(evidence).save(path)
        print(path)
    if args.export_nonroad_svg:
        nonroad = load_nonroad_evidence()
        root, paths, _, _ = _controls()
        path = resolve_repo_path(root, paths.artifacts["documentation_assets"].path) / f"{nonroad.settings.figure_id}.svg"
        path.parent.mkdir(parents=True, exist_ok=True)
        nonroad_efficiency_chart(nonroad).save(path)
        print(path)
    if not any((args.write_report, args.write_nonroad_report, args.export_svg, args.export_nonroad_svg)):
        evidence = build_fleet_100()
        print(evidence.shares.to_string(index=False))


if __name__ == "__main__":
    main()
