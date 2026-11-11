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
from utils import (ConfigBundle, file_sha256, load_config_bundle, load_harmonization_rules,
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
        "activity_calibration": resolve_artifact_path(bundle, "road_utilization_processed", utilization["sales_calibration_file"]),
    }
    for name in ("annual", "periods", "split_evidence", "ratings", "atb"):
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


def load_calibration_diagnostic(*, repo_root: Path | None = None) -> CEUDCalibrationDiagnostic:
    """Read one immutable successful build; never run ETL or select live YAML."""
    captured = load_notebook_evidence("ceud_calibration", repo_root=repo_root)
    frames = {name: pd.read_csv(path, low_memory=False) for name, path in captured.paths.items() if path.name.endswith((".csv", ".csv.gz"))}
    audit = json.loads(captured.paths["integrity"].read_text(encoding="utf-8"))
    if "ceud_energy_calibration" not in audit:
        raise ValueError("Captured efficiency audit lacks the optional CEUD calibration")
    if "representative_audit" in captured.paths:
        audit["representative"] = json.loads(captured.paths["representative_audit"].read_text(encoding="utf-8"))
    validate_calibration_evidence(frames)
    return CEUDCalibrationDiagnostic(frames, captured.metadata, audit)


def calibration_charts(diagnostic: CEUDCalibrationDiagnostic, *, region: str,
                       road_class: str = "cars", powertrain: str = "gasoline") -> dict[str, alt.TopLevelMixin]:
    """Return the notebook's exact static views for publication with fixed selections."""
    summary = diagnostic.frames["summary"].loc[lambda frame: frame.region.eq(region)].copy()
    summary["vehicle_class"] = summary.road_class.map(CLASS_LABELS)
    summary["calibration_status"] = summary.status.map({"calibrate": "Fitted to liquid energy", "unsupported_fuel_boundary": "Fuel boundary unavailable"})
    if summary.empty:
        raise ValueError(f"No captured CEUD calibration evidence for region {region}")
    order = list(CLASS_LABELS.values())
    fitted = summary.loc[summary.status.eq("calibrate")]
    comparison = fitted.melt(id_vars=["vehicle_class"], value_vars=[
        "unadjusted_liquid_energy_pj", "calibrated_liquid_energy_pj", "ceud_liquid_target_pj"],
        var_name="basis", value_name="energy_pj")
    comparison["basis"] = comparison.basis.map({"unadjusted_liquid_energy_pj": "Unadjusted",
        "calibrated_liquid_energy_pj": "Calibrated", "ceud_liquid_target_pj": "NRCan CEUD target"})
    energy = alt.Chart(comparison).mark_bar().encode(
        x=alt.X("vehicle_class:N", title=None, sort=order, axis=alt.Axis(labelAngle=-15)),
        xOffset=alt.XOffset("basis:N", sort=["Unadjusted", "Calibrated", "NRCan CEUD target"]),
        y=alt.Y("energy_pj:Q", title="Gasoline + diesel energy (PJ)", scale=alt.Scale(zero=True)),
        color=alt.Color("basis:N", title=None, scale=alt.Scale(domain=["Unadjusted", "Calibrated", "NRCan CEUD target"], range=["#64748b", "#087e8b", "#c56824"]),
                        legend=alt.Legend(labelFontStyle=source_legend_style())),
        tooltip=["vehicle_class:N", "basis:N", alt.Tooltip("energy_pj:Q", format=".3f")],
    ).properties(width=700, height=280)
    multiplier = alt.Chart(summary).mark_bar().encode(
        y=alt.Y("vehicle_class:N", title=None, sort=order),
        x=alt.X("consumption_multiplier:Q", title="Consumption multiplier (dimensionless)", scale=alt.Scale(zero=True), axis=alt.Axis(tickCount=6)),
        color=alt.Color("calibration_status:N", title=None, scale=alt.Scale(domain=["Fitted to liquid energy", "Fuel boundary unavailable"], range=["#087e8b", "#94a3b8"]),
                        legend=alt.Legend(values=sorted(summary.calibration_status.unique()))),
        tooltip=["vehicle_class:N", "status:N", alt.Tooltip("consumption_multiplier:Q", format=".4f")],
    )
    reference = alt.Chart(pd.DataFrame({"reference": [1.]})).mark_rule(strokeDash=[4, 4], color="#334155").encode(x="reference:Q")
    boundary = summary.assign(covered=summary.ceud_liquid_target_pj.fillna(0)).melt(
        id_vars=["vehicle_class"], value_vars=["covered", "excluded_reported_energy_pj", "other_or_unresolved_energy_pj"],
        var_name="boundary", value_name="energy_pj")
    boundary["boundary"] = boundary.boundary.map({"covered": "Comparable gasoline + diesel", "excluded_reported_energy_pj": "Excluded propane / natural gas", "other_or_unresolved_energy_pj": "Unresolved fuel boundary"})
    boundary_chart = alt.Chart(boundary).mark_bar().encode(
        y=alt.Y("vehicle_class:N", title=None, sort=order),
        x=alt.X("energy_pj:Q", title="Reported class energy (PJ)", stack="zero", axis=alt.Axis(tickCount=8)),
        color=alt.Color("boundary:N", title=None, scale=alt.Scale(domain=["Comparable gasoline + diesel", "Excluded propane / natural gas", "Unresolved fuel boundary"], range=["#087e8b", "#c56824", "#94a3b8"])),
        tooltip=["vehicle_class:N", "boundary:N", alt.Tooltip("energy_pj:Q", format=".3f")],
    ).properties(width=700, height=250)
    trajectory = diagnostic.frames["trajectories"].loc[lambda frame: frame.region.eq(region)
        & frame["mode"].eq(road_class) & frame.powertrain.eq(powertrain)].copy()
    if trajectory.empty:
        raise ValueError(f"No captured efficiency trajectory: {region}/{road_class}/{powertrain}")
    trajectory["cohort"] = np.where(trajectory.tech.str.endswith("_EX"), "Existing cohorts", "New vintages")
    long = trajectory.melt(id_vars=["tech", "vintage", "cohort", "native_unit"],
                           value_vars=["unadjusted_native_intensity", "native_intensity"],
                           var_name="basis", value_name="consumption")
    long["basis"] = long.basis.map({"unadjusted_native_intensity": "Unadjusted", "native_intensity": "Calibrated"})
    unit = trajectory.native_unit.unique()
    if len(unit) != 1:
        raise ValueError("CEUD diagnostic trajectory cannot mix units")
    line_base = alt.Chart(long).encode(
        x=alt.X("vintage:Q", title="Model vintage", axis=alt.Axis(format="d")),
        y=alt.Y("consumption:Q", title=f"Consumption ({unit[0]})", scale=alt.Scale(zero=False)),
        color=alt.Color("basis:N", title=None, scale=alt.Scale(domain=["Unadjusted", "Calibrated"], range=["#64748b", "#087e8b"])),
        detail=["tech:N", "basis:N"], order="vintage:Q",
        tooltip=["tech:N", "vintage:O", "basis:N", alt.Tooltip("consumption:Q", format=".4f")],
    )
    line = (line_base.mark_line(strokeWidth=2).encode(
        strokeDash=alt.StrokeDash("cohort:N", title="Cohort", scale=alt.Scale(
            domain=["Existing cohorts", "New vintages"], range=[[1, 0], [6, 3]]),
            legend=alt.Legend(symbolType="stroke", symbolSize=300, symbolStrokeWidth=2)),
    ) + line_base.mark_point(filled=True, size=25)).resolve_legend(
        strokeDash="independent", color="shared").properties(width=700, height=280)
    charts = {
        "ceud-cal-1": identified_chart(energy, "ceud-cal-1", "Liquid energy closes while fleet activity stays fixed"),
        "ceud-cal-2": identified_chart((multiplier + reference).properties(width=700, height=250), "ceud-cal-2", "One class multiplier changes every represented consumption level"),
        "ceud-cal-3": identified_chart(boundary_chart, "ceud-cal-3", "Reported fuel coverage limits the calibration target"),
        "ceud-cal-4": identified_chart(line, "ceud-cal-4", f"{CLASS_LABELS[road_class]}: {powertrain} trajectory keeps its relative change"),
    }
    charts.update(calibration_anchor_charts(diagnostic))
    for chart in charts.values():
        chart.to_dict(validate=True)
    return charts


def calibration_anchor_charts(diagnostic: CEUDCalibrationDiagnostic) -> dict[str, alt.TopLevelMixin]:
    """Compare prepared base-year range-anchor efficiencies across captured regions.

    Every cell is one region/class/powertrain at model vintage base_year. The
    percentage scale permits comparisons across passenger and freight service
    units; before/after prepared range-anchor efficiencies remain in tooltips.
    These explicit range rows precede representative aggregation. Its common
    class multiplier preserves the same percentage adjustment in final rows.
    An unsupported
    class is labelled pending, rather than presented as an independently verified
    zero adjustment.
    """
    summary = diagnostic.frames["summary"][["region", "road_class", "base_year", "status"]]
    anchors = diagnostic.frames["trajectories"].merge(
        summary, left_on=["region", "mode"], right_on=["region", "road_class"],
        how="left", validate="many_to_one")
    anchors = anchors.loc[anchors.vintage.eq(anchors.base_year)].copy()
    if anchors.empty:
        return {}
    if anchors.duplicated(["region", "road_class", "powertrain"]).any():
        raise ValueError("CEUD base-year anchor has ambiguous region/class/powertrain efficiencies")
    anchors["vehicle_class"] = anchors.road_class.map(CLASS_LABELS)
    anchors["efficiency_change"] = anchors.efficiency / anchors.unadjusted_efficiency - 1
    anchors["cell_label"] = [f"{value:+.1%}" if status == "calibrate" else "pending"
                             for value, status in zip(anchors.efficiency_change, anchors.status, strict=True)]
    maximum = max(float(anchors.efficiency_change.abs().max()), .01)
    charts = {}
    for group, car_selection in [("cars", True), ("trucks", False)]:
        selected = anchors.loc[anchors.road_class.eq("cars") == car_selection].copy()
        if selected.empty:
            continue
        base = alt.Chart(selected).encode(
            x=alt.X("powertrain:N", title="Powertrain / range", axis=alt.Axis(labelAngle=-35, labelLimit=180)),
            y=alt.Y("region:N", title="Region", sort=sorted(selected.region.unique())),
            tooltip=["region:N", "vehicle_class:N", "powertrain:N", "base_year:O", "status:N",
                     alt.Tooltip("unadjusted_efficiency:Q", title="Prepared range anchor before", format=".5g"),
                     alt.Tooltip("efficiency:Q", title="Prepared range anchor after", format=".5g"), "units:N",
                     alt.Tooltip("efficiency_change:Q", title="Efficiency change", format="+.2%")],
        )
        tiles = base.mark_rect(stroke="white", strokeWidth=1).encode(
            color=alt.condition(alt.datum.status == "calibrate",
                alt.Color("efficiency_change:Q", title="Efficiency change", scale=alt.Scale(
                    domain=[-maximum, maximum], scheme="redblue"), legend=alt.Legend(format="+.0%")),
                alt.value("#e2e8f0")))
        labels = base.mark_text(fontSize=9).encode(
            text="cell_label:N", color=alt.condition(
                (alt.datum.status == "calibrate") & (abs(alt.datum.efficiency_change) > maximum * .6),
                alt.value("white"), alt.value("#172033")))
        chart = (tiles + labels).properties(width=720, height=max(70, 25 * selected.region.nunique()))
        if not car_selection:
            chart = chart.facet(row=alt.Row("vehicle_class:N", title=None,
                sort=[value for key, value in CLASS_LABELS.items() if key != "cars"],
                header=alt.Header(labelAngle=0, labelAlign="left", labelLimit=180)), spacing=30)
        identity = "ceud-cal-5-" + group
        title = f"Prepared base-year range-anchor efficiency changes: {group}"
        years = ", ".join(str(int(year)) for year in sorted(selected.base_year.unique()))
        chart = chart.properties(title=alt.Title(title, subtitle=[
            f"Model vintage {years} only; explicit ranges before representative aggregation"], anchor="start"))
        chart = identified_chart(chart, identity, title)
        chart.to_dict(validate=True)
        charts[identity] = chart
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
    off_config = {key: recorded_config[key]["sha256"] for key in ("paths", "sources")}
    off_config.update({Path(entry["path"]).stem: entry["sha256"] for entry in recorded_config["parameters"]})
    for key, digest in off_config.items():
        if digest != on.metadata["config"][key]["sha256"]:
            raise ValueError(f"Off/on captured configuration differs: {key}")
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
