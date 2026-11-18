"""Read-only CEUD calibration evidence and reusable, identified Altair views."""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import json
from pathlib import Path
import sqlite3

import altair as alt
import numpy as np
import pandas as pd

from parameterization.ceud_energy_calibration import validate_emitted_calibration as validate_calibration_evidence
from parameterization.road_efficiencies import RoadEfficiencyEvidence
from utils import (ConfigBundle, file_sha256, load_config_bundle, load_conversion_factors, load_harmonization_rules,
                   resolve_artifact_path, resolve_input_path, write_text_atomic)
from validation.notebook_evidence import load_notebook_evidence
from validation.notebook_figures import identified_chart, source_legend_style


CLASS_LABELS = {
    "cars": "Cars", "passenger_light_trucks": "Passenger light trucks",
    "freight_light_trucks": "Freight light trucks", "medium_trucks": "Medium trucks",
    "heavy_trucks": "Heavy trucks",
}


@dataclass(frozen=True)
class CEUDCalibrationDiagnostic:
    frames: dict[str, pd.DataFrame]
    metadata: dict
    audit: dict


def collect_calibration_inputs(bundle: ConfigBundle) -> dict[str, Path]:
    """Name already prepared inputs for the caller-owned successful-build capture."""
    if not bundle.scenario.efficiencies.ceud_energy_calibration:
        raise ValueError("CEUD calibration capture requires an explicitly enabled scenario")
    rules = load_harmonization_rules(bundle, "ceud_energy_calibration")
    efficiency = load_harmonization_rules(bundle, "efficiencies")
    utilization = load_harmonization_rules(bundle, "road_utilization")
    paths = {
        "summary": resolve_artifact_path(bundle, "efficiencies_validation", rules["files"]["summary"]),
        "cohorts": resolve_artifact_path(bundle, "efficiencies_interim", rules["files"]["cohorts"]),
        "trajectories": resolve_artifact_path(bundle, "efficiencies_interim", rules["files"]["trajectories"]),
        "integrity": resolve_artifact_path(bundle, "efficiencies_validation", efficiency["files"]["integrity"]),
        "activity_calibration": resolve_artifact_path(bundle, "road_utilization_processed", utilization["base_year_calibration_file"]),
    }
    for name in ("annual", "periods", "split_evidence", "ratings", "rating_weights", "loads", "atb"):
        paths[name] = resolve_artifact_path(bundle, "efficiencies_interim", efficiency["files"][name])
    for name in ("efficiency", "splits"):
        paths[name] = resolve_artifact_path(bundle, "efficiencies_processed", efficiency["files"][name])
    ceud = load_harmonization_rules(bundle, "nrcan_ceud")
    for region in bundle.scenario.geography.regions:
        paths["source_ceud_" + region.lower()] = resolve_input_path(
            bundle, "interim", ceud["interim_subdir"], ceud["region_output_template"].format(region=region.lower()))
    if bundle.scenario.BEV_PHEV_range_representation.mode == "representative_archetype":
        ranges = load_harmonization_rules(bundle, "ldv_ev_ranges")
        paths["representative_audit"] = resolve_artifact_path(
            bundle, "ldv_range_validation", ranges["representative"]["audit_file"])
    return paths


def _load_calibration_profile(profile: str, *, repo_root: Path | None = None) -> CEUDCalibrationDiagnostic:
    captured = load_notebook_evidence(profile, repo_root=repo_root)
    frames = {name: pd.read_csv(path, low_memory=False) for name, path in captured.paths.items() if path.name.endswith((".csv", ".csv.gz"))}
    audit = json.loads(captured.paths["integrity"].read_text(encoding="utf-8"))
    if "ceud_energy_calibration" not in audit:
        raise ValueError("Captured efficiency audit lacks the optional CEUD calibration")
    if "representative_audit" in captured.paths:
        audit["representative"] = json.loads(captured.paths["representative_audit"].read_text(encoding="utf-8"))
    validate_calibration_evidence(frames)
    controls = load_harmonization_rules(captured.bundle, "notebook_diagnostics")
    years = controls.get("ceud_calibration", {}).get("trajectory_years", [
        min(captured.bundle.scenario.periods.existing),
        captured.bundle.scenario.periods.evaluation_year(max(captured.bundle.scenario.periods.model)),
    ])
    metadata = captured.metadata | {"trajectory_years": years,
        "projection_reference_year": min(captured.bundle.scenario.periods.base_year,
            load_harmonization_rules(captured.bundle, "efficiencies")["projection_reference_year"])}
    if profile == "ceud_calibration_regions":
        expected = controls["ceud_calibration_regions"]["expected_regions"]
        region_map = load_harmonization_rules(captured.bundle, "road_stocks_and_demands")["existing_capacity"]["region_output_map"]
        require_regional_coverage(frames, expected_regions=expected, region_output_map=region_map)
        metadata["expected_regions"] = [region_map.get(region, region) for region in expected]
        metadata["source_regions"] = expected
    if {"rating_weights", "loads"}.issubset(frames):
        frames["source_trajectories"] = source_consumption_trajectories(captured.bundle, frames, years=years)
        metadata["source_history_before_stock_eligibility"] = True
    elif profile == "ceud_calibration_regions":
        raise FileNotFoundError("Regional CEUD capture lacks pre-eligibility source history inputs. "
                                f"Rebuild with: {captured.metadata['prepare_command']}")
    else:
        # Older Ontario captures remain valid paired-comparison evidence. They
        # cannot establish the source history of cohorts absent from that capture.
        frames["source_trajectories"] = frames["annual"]
        metadata["source_history_before_stock_eligibility"] = False
    return CEUDCalibrationDiagnostic(frames, metadata, audit)


def load_calibration_diagnostic(*, repo_root: Path | None = None) -> CEUDCalibrationDiagnostic:
    """Read the matched Ontario calibration capture without preparing inputs."""
    return _load_calibration_profile("ceud_calibration", repo_root=repo_root)


def load_regional_calibration_diagnostic(*, repo_root: Path | None = None) -> CEUDCalibrationDiagnostic:
    """Require the separate complete regional capture; never fall back to Ontario."""
    return _load_calibration_profile("ceud_calibration_regions", repo_root=repo_root)


def require_regional_coverage(frames: dict[str, pd.DataFrame], *, expected_regions: list[str],
                              region_output_map: dict[str, str] | None = None) -> None:
    """Reject partial class/region evidence before presenting an all-region view."""
    if (not isinstance(expected_regions, list) or not expected_regions
            or any(not isinstance(region, str) or not region.strip() or region != region.strip()
                   for region in expected_regions)
            or len(set(expected_regions)) != len(expected_regions)):
        raise ValueError("Regional CEUD expected_regions must be a nonempty list of distinct region-code strings; quote ON in YAML")
    mapping = region_output_map or {}
    model_regions = [mapping.get(region, region) for region in expected_regions]
    if (any(not isinstance(region, str) or not region for region in model_regions)
            or len(set(model_regions)) != len(model_regions)):
        raise ValueError("Regional CEUD source regions must resolve to distinct model-region strings")
    summary = frames["summary"]
    expected = {(region, mode) for region in model_regions for mode in CLASS_LABELS}
    actual = set(summary[["region", "road_class"]].itertuples(index=False, name=None))
    if actual != expected or summary.duplicated(["region", "road_class"]).any():
        raise ValueError(f"Regional CEUD calibration coverage is incomplete: missing {sorted(expected - actual)}; unexpected {sorted(actual - expected)}")
    anchors = frames["trajectories"].merge(summary[["region", "road_class", "base_year"]],
        left_on=["region", "mode"], right_on=["region", "road_class"], how="left", validate="many_to_one")
    actual_anchors = set(anchors.loc[anchors.vintage.eq(anchors.base_year), ["region", "road_class"]]
                         .itertuples(index=False, name=None))
    if actual_anchors != expected:
        raise ValueError(f"Regional CEUD base-year anchors are incomplete: {sorted(expected - actual_anchors)}")


def source_consumption_trajectories(bundle: ConfigBundle, frames: dict[str, pd.DataFrame], *, years: list[int]) -> pd.DataFrame:
    """Extend the diagnostic history before stock eligibility using the owning method.

    These are source-year calculations, not extra compiled capacity/efficiency rows.
    Future points retain the captured annual values and their source-year selection.
    """
    rules = load_harmonization_rules(bundle, "efficiencies")
    ceud = pd.concat([frame for key, frame in frames.items() if key.startswith("source_ceud_")], ignore_index=True)
    conversion = load_conversion_factors(bundle)
    evidence = RoadEfficiencyEvidence(ceud=ceud, loads=frames["loads"], rating_audit=frames["rating_weights"],
        atb_audit=frames["atb"], gcam=pd.DataFrame(), regen=pd.DataFrame(),
        base_year=bundle.scenario.periods.base_year, rules=rules, conversions=conversion)
    incumbent = {mode: "gasoline" if mode in {"cars", "passenger_light_trucks", "freight_light_trucks"} else "diesel"
                 for mode in CLASS_LABELS}
    annual = frames["annual"]
    selected = pd.Series(False, index=annual.index)
    for mode, powertrain in incumbent.items():
        selected |= annual["mode"].eq(mode) & annual.powertrain.eq(powertrain)
    columns = ["region", "mode", "powertrain", "source_year", "native_unit",
               "unadjusted_native_intensity", "native_intensity", "treatment"]
    prepared = annual.loc[selected & annual.source_year.between(*years), columns].copy()
    keys = ["region", "mode", "powertrain", "source_year"]
    if prepared.duplicated(keys).any():
        raise ValueError("Ambiguous incumbent annual calibration source years")
    prepared["evidence_basis"] = "Prepared annual source value"
    records = []
    for row in frames["summary"].itertuples(index=False):
        fuel = incumbent[row.road_class]
        for year in range(years[0], min(years[1], evidence.observation_end_year) + 1):
            result = evidence.derive_consumption(row.ceud_region, row.road_class, fuel, year)
            if result is None:
                continue
            present = prepared.loc[prepared.region.eq(row.region) & prepared["mode"].eq(row.road_class)
                                   & prepared.powertrain.eq(fuel) & prepared.source_year.eq(year)]
            if not present.empty:
                if not np.allclose(present.unadjusted_native_intensity, result["native_intensity"], rtol=1e-10, atol=1e-12):
                    raise ValueError(f"Source-year diagnostic differs from prepared consumption: {row.region}/{row.road_class}/{year}")
                continue
            records.append({"region": row.region, "mode": row.road_class, "powertrain": fuel,
                "source_year": year, "native_unit": result["native_unit"],
                "unadjusted_native_intensity": result["native_intensity"],
                "native_intensity": result["native_intensity"] * row.consumption_multiplier,
                "treatment": result["treatment"], "evidence_basis": "Source history before stock eligibility"})
    return pd.concat([prepared, pd.DataFrame(records)], ignore_index=True).sort_values(keys).reset_index(drop=True)


def calibration_charts(diagnostic: CEUDCalibrationDiagnostic, *, region: str,
                       include_anchors: bool = True) -> dict[str, alt.TopLevelMixin]:
    """Return the combined fit and all-incumbent source-year trajectories.

    The trajectory comparison includes all incumbent road classes. Heatmaps use
    all captured regions independently of the selected energy-comparison region.
    """
    summary = diagnostic.frames["summary"].loc[lambda frame: frame.region.eq(region)].copy()
    if summary.empty:
        raise ValueError(f"No captured CEUD calibration evidence for region {region}")
    summary["vehicle_class"] = summary.road_class.map(CLASS_LABELS)
    charts = {
        "ceud-cal-1": identified_chart(_energy_fit_chart(summary), "ceud-cal-1",
            "Liquid-energy fit, excluded fuels and the consumption multiplier"),
        "ceud-cal-4": identified_chart(_incumbent_trajectory_chart(diagnostic, region), "ceud-cal-4",
            "Incumbent consumption trajectories before and after calibration"),
    }
    if include_anchors:
        charts.update(calibration_anchor_charts(diagnostic))
    for chart in charts.values():
        chart.to_dict(validate=True)
    return charts


def _energy_fit_chart(summary: pd.DataFrame) -> alt.TopLevelMixin:
    """Use explicit bar geometry so diagonal stripes stay inside excluded energy."""
    order = list(CLASS_LABELS.values())
    fitted = summary.loc[summary.status.eq("calibrate")].copy()
    bases = ["Unadjusted", "NRCan CEUD target", "Calibrated"]
    colors = ["#64748b", "#c56824", "#16823b"]
    fields = ["unadjusted_liquid_energy_pj", "ceud_liquid_target_pj", "calibrated_liquid_energy_pj"]
    bars, exclusions, markers = [], [], []
    for row in fitted.itertuples(index=False):
        centre = order.index(row.vehicle_class) * 4
        for offset, basis, field in zip([-1, 0, 1], bases, fields, strict=True):
            bars.append({"vehicle_class": row.vehicle_class, "basis": basis,
                "x1": centre + offset - .34, "x2": centre + offset + .34,
                "lower": 0., "upper": float(getattr(row, field))})
        excluded = float(getattr(row, "excluded_reported_energy_pj", 0.) or 0.)
        if excluded > 0:
            exclusions.append({"vehicle_class": row.vehicle_class, "basis": "NRCan CEUD target",
                "x1": centre - .34, "x2": centre + .34,
                "lower": row.ceud_liquid_target_pj, "upper": row.ceud_liquid_target_pj + excluded,
                "excluded": excluded})
        markers.append({"vehicle_class": row.vehicle_class, "position": centre,
                        "consumption_multiplier": row.consumption_multiplier,
                        "indicator": "Consumption multiplier"})
    if not bars:
        raise ValueError("No fitted liquid-energy targets to plot")
    width, height = 900, 310
    x_domain = [-2, 4 * (len(order) - 1) + 2]
    y_max = max(row["upper"] for row in [*bars, *exclusions]) * 1.12
    axis = alt.Axis(values=[4 * i for i in range(len(order))],
        labelExpr=f"{json.dumps(order)}[datum.value / 4]", labelAngle=-12, labelLimit=200,
        ticks=False, grid=False)
    x = alt.X("x1:Q", title=None, scale=alt.Scale(domain=x_domain), axis=axis)
    y = alt.Y("upper:Q", title="Annual energy (PJ)",
              scale=alt.Scale(domain=[0, y_max], zero=True))
    energy = alt.Chart(pd.DataFrame(bars)).mark_rect().encode(x=x, x2="x2:Q", y=y, y2="lower:Q",
        color=alt.Color("basis:N", title=None, sort=bases,
            scale=alt.Scale(domain=bases, range=colors),
            legend=alt.Legend(orient="bottom", labelFontStyle=source_legend_style())),
        tooltip=["vehicle_class:N", "basis:N", alt.Tooltip("upper:Q", title="Energy (PJ)", format=".3f")])
    layers = [energy]
    if exclusions:
        excluded = alt.Chart(pd.DataFrame(exclusions)).mark_rect(fill=colors[1], fillOpacity=.12,
            stroke=colors[1], strokeWidth=1).encode(x=x, x2="x2:Q", y=y, y2="lower:Q",
            tooltip=["vehicle_class:N", alt.Tooltip("excluded:Q", title="Excluded propane / natural gas (PJ)", format=".3f")])
        stripes = []
        # A 45-degree line in the rendered chart; clip every segment to its bar.
        slope = y_max / height * width / (x_domain[1] - x_domain[0])
        step = y_max / height * 7
        for bar in exclusions:
            for origin in np.arange(bar["lower"] - slope * (bar["x2"] - bar["x1"]), bar["upper"], step):
                left = max(bar["x1"], bar["x1"] + (bar["lower"] - origin) / slope)
                right = min(bar["x2"], bar["x1"] + (bar["upper"] - origin) / slope)
                if right > left:
                    stripes.append({"x1": left, "x2": right,
                        "lower": origin + slope * (left - bar["x1"]),
                        "upper": origin + slope * (right - bar["x1"])})
        layers.append(excluded)
        if stripes:
            layers.append(alt.Chart(pd.DataFrame(stripes)).mark_rule(color=colors[1], strokeWidth=1).encode(
                x=x, x2="x2:Q", y=y, y2="lower:Q"))
    point = alt.Chart(pd.DataFrame(markers)).mark_point(filled=True, color="#172033", size=85,
        stroke="white", strokeWidth=.8).encode(
        x=alt.X("position:Q", title=None, scale=alt.Scale(domain=x_domain), axis=axis),
        y=alt.Y("consumption_multiplier:Q", title="Consumption multiplier (×)",
            scale=alt.Scale(zero=True), axis=alt.Axis(orient="right", format=".2f", grid=False)),
        shape=alt.Shape("indicator:N", title=None, scale=alt.Scale(
            domain=["Consumption multiplier"], range=["diamond"]), legend=alt.Legend(orient="bottom")),
        tooltip=["vehicle_class:N", alt.Tooltip("consumption_multiplier:Q", format=".4f")])
    point_labels = point.mark_text(color="#172033", stroke=None, dy=-11, fontSize=10).encode(
        text=alt.Text("consumption_multiplier:Q", format=".3f"))
    return alt.layer(alt.layer(*layers), alt.layer(point, point_labels)).resolve_scale(
        y="independent").properties(width=width, height=height)


def _incumbent_trajectory_chart(diagnostic: CEUDCalibrationDiagnostic, region: str) -> alt.TopLevelMixin:
    frame = diagnostic.frames.get("source_trajectories", diagnostic.frames.get("annual"))
    if frame is None:
        frame = diagnostic.frames["trajectories"].rename(columns={"vintage": "source_year"})
    frame = frame.loc[frame.region.eq(region)].copy()
    incumbent = {key: "gasoline" if key in {"cars", "passenger_light_trucks", "freight_light_trucks"}
                 else "diesel" for key in CLASS_LABELS}
    selected = pd.Series(False, index=frame.index)
    for mode, fuel in incumbent.items():
        selected |= frame["mode"].eq(mode) & frame.powertrain.eq(fuel)
    frame = frame.loc[selected].copy()
    if frame.empty:
        raise ValueError(f"No incumbent consumption trajectories for {region}")
    if frame.duplicated(["mode", "source_year"]).any():
        raise ValueError("Ambiguous incumbent consumption source years")
    if set(frame.native_unit) != {"MJ/vkm"}:
        raise ValueError("Incumbent calibration trajectories must share MJ/vkm units")
    frame["vehicle_class"] = frame["mode"].map(CLASS_LABELS)
    if "evidence_basis" not in frame:
        frame["evidence_basis"] = "Captured prepared source value"
    long = frame.melt(id_vars=["vehicle_class", "powertrain", "source_year", "evidence_basis"],
        value_vars=["unadjusted_native_intensity", "native_intensity"],
        var_name="basis", value_name="consumption")
    long["basis"] = long.basis.map({"unadjusted_native_intensity": "Unadjusted", "native_intensity": "Calibrated"})
    years = diagnostic.metadata.get("trajectory_years", [float(frame.source_year.min()), float(frame.source_year.max())])
    ticks = sorted(set(range((int(years[0]) + 4) // 5 * 5, int(years[1]) + 1, 5)) | {int(years[0]), int(years[1])})
    base = alt.Chart(long).encode(
        x=alt.X("source_year:Q", title="Source year", scale=alt.Scale(domain=years), axis=alt.Axis(format="d", values=ticks)),
        y=alt.Y("consumption:Q", title="Consumption (MJ/vehicle-km)", scale=alt.Scale(zero=False)),
        color=alt.Color("vehicle_class:N", title="Road class", sort=list(CLASS_LABELS.values()),
            scale=alt.Scale(domain=list(CLASS_LABELS.values()),
                range=["#2166ac", "#1b9e77", "#66a61e", "#d95f02", "#7570b3"]),
            legend=alt.Legend(orient="bottom")),
        detail=["vehicle_class:N", "basis:N"], order="source_year:Q",
        tooltip=["vehicle_class:N", "powertrain:N", "source_year:O", "basis:N", "evidence_basis:N",
                 alt.Tooltip("consumption:Q", format=".4f")])
    lines = base.mark_line(strokeWidth=2.3).encode(strokeDash=alt.StrokeDash("basis:N", title="Calibration",
        scale=alt.Scale(domain=["Unadjusted", "Calibrated"], range=[[], [7, 4]]),
        legend=alt.Legend(orient="bottom", symbolType="stroke", symbolStrokeWidth=2.3, symbolSize=350)))
    return (lines + base.mark_point(filled=True, size=18)).resolve_legend(
        color="shared", strokeDash="independent").properties(width=900, height=310)


def calibration_anchor_charts(diagnostic: CEUDCalibrationDiagnostic) -> dict[str, alt.TopLevelMixin]:
    """One row per province/model region, faceted by class within LDV and MHDV."""
    summary = diagnostic.frames["summary"][["region", "road_class", "base_year", "status"]]
    anchors = diagnostic.frames["trajectories"].merge(summary,
        left_on=["region", "mode"], right_on=["region", "road_class"], how="left", validate="many_to_one")
    anchors = anchors.loc[anchors.vintage.eq(anchors.base_year)].copy()
    if anchors.empty:
        return {}
    if anchors.duplicated(["region", "road_class", "powertrain"]).any():
        raise ValueError("CEUD base-year anchor has ambiguous region/class/powertrain efficiencies")
    anchors["vehicle_class"] = anchors.road_class.map(CLASS_LABELS)
    anchors["efficiency_change"] = anchors.efficiency / anchors.unadjusted_efficiency - 1
    maximum = max(float(anchors.efficiency_change.abs().max()), .01)
    regions = diagnostic.metadata.get("expected_regions", sorted(summary.region.unique()))
    charts = {}
    for group, modes in [("ldv", ["cars", "passenger_light_trucks", "freight_light_trucks"]),
                         ("mhdv", ["medium_trucks", "heavy_trucks"])]:
        selected = anchors.loc[anchors.road_class.isin(modes)].copy()
        if selected.empty:
            continue
        classes = [CLASS_LABELS[key] for key in modes if key in set(selected.road_class)]
        fuels = sorted(selected.powertrain.unique())
        grid = pd.MultiIndex.from_product([regions, classes, fuels],
            names=["region", "vehicle_class", "powertrain"]).to_frame(index=False)
        selected = grid.merge(selected, on=["region", "vehicle_class", "powertrain"],
                              how="left", validate="one_to_one")
        selected["cell_label"] = [f"{value:+.1%}" if status == "calibrate" else "—" if pd.isna(status) else "pending"
            for value,status in zip(selected.efficiency_change, selected.status, strict=True)]
        base = alt.Chart(selected).encode(
            x=alt.X("powertrain:N", title="Powertrain / range", sort=fuels,
                axis=alt.Axis(labelAngle=-35, labelLimit=180)),
            y=alt.Y("region:N", title="Region", sort=regions),
            tooltip=["region:N", "vehicle_class:N", "powertrain:N", "base_year:O", "status:N",
                alt.Tooltip("unadjusted_efficiency:Q", title="Prepared range anchor before", format=".5g"),
                alt.Tooltip("efficiency:Q", title="Prepared range anchor after", format=".5g"), "units:N",
                alt.Tooltip("efficiency_change:Q", title="Efficiency change", format="+.2%")])
        tiles = base.mark_rect(stroke="white", strokeWidth=1).encode(color=alt.condition(
            alt.datum.status == "calibrate", alt.Color("efficiency_change:Q", title="Efficiency change",
                scale=alt.Scale(domain=[-maximum, maximum], scheme="redblue"), legend=alt.Legend(format="+.0%")),
            alt.value("#e2e8f0")))
        labels = base.mark_text(fontSize=9).encode(text="cell_label:N", color=alt.condition(
            (alt.datum.status == "calibrate") & (abs(alt.datum.efficiency_change) > maximum * .6),
            alt.value("white"), alt.value("#172033")))
        chart = (tiles + labels).properties(width=850, height=max(70, 23 * len(regions))).facet(
            row=alt.Row("vehicle_class:N", title=None, sort=classes,
                header=alt.Header(labelAngle=0, labelAlign="left", labelLimit=180)), spacing=25)
        identity = "ceud-cal-5-" + group
        title = f"Regional base-year efficiency changes: {'light-duty cars and trucks' if group == 'ldv' else 'medium/heavy trucks'}"
        chart = chart.properties(title=alt.Title(title, subtitle=[
            "All captured regions; BCT combines British Columbia and the territories",
            "Explicit range anchors before representative aggregation; grey/dash means no comparison"], anchor="start"))
        charts[identity] = identified_chart(chart, identity, title)
    return charts


def _aligned_parameter_rows(before: pd.DataFrame, after: pd.DataFrame,
                            keys: list[str], table: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if before.duplicated(keys).any() or after.duplicated(keys).any():
        raise ValueError(f"Duplicate effective parameter keys in {table}")
    before, after = (frame.set_index(keys).sort_index() for frame in (before, after))
    if not before.index.equals(after.index) or list(before.columns) != list(after.columns):
        raise ValueError(f"Off/on parameter coverage differs in {table}")
    return before, after


def compare_efficiency_frames(before: pd.DataFrame, after: pd.DataFrame,
                              diagnostic: CEUDCalibrationDiagnostic) -> dict:
    """Verify every final vehicle/range/vintage efficiency and unchanged other edges."""
    keys = ["region", "input_comm", "tech", "vintage", "output_comm"]
    columns = [*keys, "efficiency", "units"]
    left, right = _aligned_parameter_rows(before[columns], after[columns], keys, "efficiency")
    if not left.units.equals(right.units):
        raise ValueError("Off/on efficiency units differ")
    mapping = diagnostic.frames["trajectories"][["region", "output_comm", "mode"]].drop_duplicates()
    if mapping.duplicated(["region", "output_comm"]).any():
        raise ValueError("Ambiguous service-to-CEUD-class mapping")
    actual = right.reset_index().merge(mapping, on=["region", "output_comm"], how="left", validate="many_to_one")
    actual = actual.merge(diagnostic.frames["summary"][["region", "road_class", "consumption_multiplier"]],
                          left_on=["region", "mode"], right_on=["region", "road_class"], how="left", validate="many_to_one")
    actual["consumption_multiplier"] = actual.consumption_multiplier.fillna(1.)
    actual["unadjusted_efficiency"] = left.efficiency.to_numpy()
    expected = actual.unadjusted_efficiency / actual.consumption_multiplier
    if not np.allclose(actual.efficiency, expected, rtol=1e-10, atol=1e-12):
        raise ValueError("Final off/on efficiency changes do not match fitted class factors")
    actual["relative_error"] = abs(actual.efficiency / expected - 1)
    return {
        "rows": len(actual), "changed_rows": int((~np.isclose(actual.efficiency, actual.unadjusted_efficiency, rtol=1e-10, atol=1e-12)).sum()),
        "maximum_factor_relative_error": float(actual.relative_error.max()),
        "existing_rows": int(actual.tech.str.endswith("_EX").sum()),
        "representative_rows": int(actual.tech.str.contains("_REP_").sum()),
        "new_vintage_rows": int(actual.tech.str.endswith("_N").sum()),
        "by_class": actual.groupby("road_class", dropna=True).agg(
            rows=("tech", "size"), factor=("consumption_multiplier", "first"),
            maximum_relative_error=("relative_error", "max")).reset_index().to_dict("records"),
    }


def _require_matching_configuration(publication: dict, capture: dict) -> None:
    """Compare verified controls by file identity across the two report formats."""
    off = {key: publication[key]["sha256"] for key in ("paths", "sources")}
    on = {key: capture[key]["sha256"] for key in ("paths", "sources")}
    off.update({f"parameter/{Path(entry['path']).name}": entry["sha256"]
                for entry in publication["parameters"]})
    on.update({f"parameter/{Path(entry['path']).name}": entry["sha256"]
               for key, entry in capture.items() if key not in {"scenario", "paths", "sources"}})
    mismatches = sorted(key for key in off.keys() | on.keys() if off.get(key) != on.get(key))
    if mismatches:
        raise ValueError(f"Off/on captured configuration differs: {', '.join(mismatches)}")


def compare_calibration_databases(baseline: Path, calibrated: Path, *, baseline_report: Path,
                                  repo_root: Path | None = None) -> dict:
    """Audit supplied read-only SQLite files against matched immutable build captures.

    Only the configured review report is written. This is not ETL readiness or
    independent empirical validation; a mismatch fails without fitting new factors.
    """
    on = load_notebook_evidence("ceud_calibration", repo_root=repo_root)
    diagnostic = load_calibration_diagnostic(repo_root=repo_root)
    off_report = json.loads(baseline_report.read_text(encoding="utf-8"))
    if not off_report["ok"]:
        raise ValueError("Baseline report did not record a successful build")
    recorded_config = off_report["config"]
    # These explicit command inputs may use the original configuration only when
    # its bytes still match the successful OFF report. Never substitute a newer
    # notebook profile or silently reinterpret changed live configuration.
    for entry in [*(recorded_config[key] for key in ("scenario", "paths", "sources")),
                  *recorded_config["parameters"], *recorded_config["manual_inputs"]]:
        if file_sha256(Path(entry["path"])) != entry["sha256"]:
            raise ValueError(f"Baseline configuration changed since its build: {entry['path']}")
    off_bundle = load_config_bundle(recorded_config["scenario"]["path"], repo_root=on.bundle.repo_root,
                                   paths_path=recorded_config["paths"]["path"], sources_path=recorded_config["sources"]["path"])
    if off_bundle.scenario.efficiencies.ceud_energy_calibration or not on.bundle.scenario.efficiencies.ceud_energy_calibration:
        raise ValueError("Comparison requires an uncalibrated capture followed by a calibrated capture")
    controls = []
    for bundle in (off_bundle, on.bundle):
        scenario = bundle.scenario.model_dump()
        for key in ("scenario", "outputs", "notebook_diagnostics"):
            scenario.pop(key)
        scenario["efficiencies"].pop("ceud_energy_calibration")
        controls.append(scenario)
    if controls[0] != controls[1]:
        raise ValueError("Off/on scenarios differ beyond calibration and output identity")
    _require_matching_configuration(recorded_config, on.metadata["config"])
    identities = {}
    for name, path, expected_hash, identity in (
        ("baseline", baseline, off_report["database_sha256"], off_report["timestamp_utc"]),
        ("calibrated", calibrated, on.metadata["database"]["sha256"], on.metadata["build_id"]),
    ):
        path = path.resolve(strict=True)
        if file_sha256(path) != expected_hash:
            raise ValueError(f"Supplied {name} SQLite does not match its successful-build evidence")
        identities[name] = {"path": str(path), "sha256": expected_hash, "build_identity": identity}
    identities["baseline"]["report_sha256"] = file_sha256(baseline_report)
    source_fingerprints = []
    for audit in (off_report["efficiencies"], diagnostic.audit):
        source_fingerprints.append({entry["path"].replace("\\", "/"): entry["sha256"] for entry in audit["input_evidence"]})
    if source_fingerprints[0] != source_fingerprints[1]:
        raise ValueError("Off/on source-native efficiency input fingerprints differ")
    native = {"input_fingerprints_unchanged": True, "inputs": source_fingerprints[0],
              "limitation": "OFF preaggregation annual/period exports were not separately captured. Their byte equality is not claimed; final SQL and common-factor invariants are checked."}
    connections = [sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) for path in (baseline, calibrated)]
    try:
        before_db, after_db = connections
        tables = [row[0] for row in before_db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        preserved, parameter_frames = {}, {}
        ignored = {"data_id", "data_source", "notes", "dq_cred", "dq_geog", "dq_struc", "dq_tech", "dq_time"}
        for table in tables:
            columns = before_db.execute(f'PRAGMA table_info("{table}")').fetchall()
            if not {"dq_cred", "data_id"}.issubset({row[1] for row in columns}):
                continue
            left, right = [pd.read_sql_query(f'SELECT * FROM "{table}"', connection) for connection in connections]
            parameter_frames[table] = right
            if table == "efficiency":
                baseline_efficiency = left
                efficiency = compare_efficiency_frames(left, right, diagnostic)
                continue
            fields = [row[1] for row in columns if row[1] not in ignored]
            keys = [row[1] for row in columns if row[5] and row[1] not in ignored]
            old, new = _aligned_parameter_rows(left[fields], right[fields], keys, table)
            pd.testing.assert_frame_equal(old, new, check_exact=False, rtol=1e-10, atol=1e-12)
            differences = {}
            for field in old.select_dtypes(include="number"):
                delta = (new[field] - old[field]).abs()
                differences[field] = {"maximum_absolute_difference": float(delta.max()) if delta.notna().any() else 0.,
                                      "nonzero_differences": int(delta.gt(0).sum())}
            preserved[table] = {"rows": len(right), "keys_units_values_preserved": True,
                                "numeric_differences": differences}
        final = validate_calibration_evidence(
            diagnostic.frames | {"efficiency": parameter_frames["efficiency"], "splits": parameter_frames["limit_tech_input_split_annual"]},
            c2a=parameter_frames["capacity_to_activity"], factors=parameter_frames["limit_annual_capacity_factor"])
        integrity = after_db.execute("PRAGMA integrity_check").fetchall()
        foreign = after_db.execute("PRAGMA foreign_key_check").fetchall()
        if integrity != [("ok",)] or foreign:
            raise ValueError("Calibrated SQLite integrity or foreign keys failed")
        external = parameter_frames["efficiency"].loc[lambda frame: frame.data_source.notna()]
        dq_columns = [column for column in external if column.startswith("dq_")]
        if external[dq_columns].isna().any().any() or not external[dq_columns].isin(range(1, 6)).all().all():
            raise ValueError("Calibrated external efficiency rows lack complete DQ")
        sources = pd.read_sql_query("SELECT * FROM data_source", after_db)
        component_evidence = []
        for source_frame in (pd.read_sql_query("SELECT * FROM data_source", before_db), sources):
            component_evidence.append({data_id: {
                (row.source_id, line.strip()) for row in group.itertuples()
                for line in str(row.notes).splitlines() if line.strip()
            } for data_id, group in source_frame.groupby("data_id")})
        efficiency_keys = ["region", "input_comm", "tech", "vintage", "output_comm"]
        after_ids = parameter_frames["efficiency"].set_index(efficiency_keys).data_id
        for key, row in baseline_efficiency.set_index(efficiency_keys).iterrows():
            if not component_evidence[0].get(row.data_id, set()).issubset(component_evidence[1].get(after_ids.loc[key], set())):
                raise ValueError("A calibrated efficiency row lost baseline source/component evidence")
        with_ceud = set(sources.loc[sources.notes.fillna("").str.startswith("nrcan_ceud_transport_provincial:"), "data_id"])
        service = set(diagnostic.frames["trajectories"].output_comm)
        adjusted = external.loc[external.output_comm.isin(service)]
        if not set(adjusted.data_id).issubset(with_ceud):
            raise ValueError("Calibrated vehicle rows lack CEUD component provenance")
        provenance = {"external_efficiency_rows": len(external), "complete_dq": True,
                      "ceud_supported_vehicle_rows": len(adjusted), "integrity": "ok", "foreign_key_violations": 0,
                      "baseline_source_component_evidence_preserved": True,
                      "registration_interpretation": "Shared class-level contributor unions consolidate equivalent provenance contexts; effective parameter keys are unchanged."}
        provenance["registration_rows"] = {table: {label: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                                                   for label, connection in zip(("baseline", "calibrated"), connections, strict=True)}
                                            for table in ("data_set", "data_source")}
    finally:
        for connection in connections:
            connection.close()
    representative = diagnostic.audit.get("representative", {})
    report = {"status": "pass", "interpretation": "Matched implementation/calibration check; not independent validation",
              "builds": identities, "source_native_and_unadjusted": native, "non_efficiency_parameters": preserved,
              "efficiency": efficiency, "final_liquid_accounting": final, "provenance": provenance,
              "representative": {key: representative.get(key) for key in (
                  "max_conservation_error", "max_phev_component_relative_difference", "phev_component_energy")},
              "phev_interpretation": "Both components scale together; unchanged horizon-mean splits retain their existing per-vintage approximation."}
    output = resolve_artifact_path(on.bundle, "ceud_calibration_report")
    output.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", output)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--calibrated", type=Path, required=True)
    parser.add_argument("--baseline-report", type=Path, required=True)
    args = parser.parse_args()
    result = compare_calibration_databases(args.baseline, args.calibrated, baseline_report=args.baseline_report)
    print(json.dumps({"status": result["status"], "efficiency": result["efficiency"],
                      "final_liquid_accounting": result["final_liquid_accounting"]}, indent=2))


if __name__ == "__main__":
    main()
