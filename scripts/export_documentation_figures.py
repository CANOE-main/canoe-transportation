"""Explicitly export selected notebook figures; never prepare transport inputs."""

from __future__ import annotations

import argparse
from datetime import datetime, UTC
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import re
import subprocess
import tempfile

import altair as alt
import yaml

from utils import file_sha256
ROOT = Path(__file__).resolve().parents[1]


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def matches(identity, prefix):
    return identity == prefix or identity.startswith(prefix if prefix.endswith("-") else prefix + "-")


def notebook_controls(definitions):
    """Read selector values, excluding chart/table selections and their raw rows."""
    control_types = {"dropdown", "radio", "multiselect", "slider", "range_slider", "number",
                     "checkbox", "switch", "text", "text_area", "dictionary", "array"}
    result = {}
    for name, value in definitions.items():
        if type(value).__module__.startswith("marimo.") and type(value).__name__ in control_types:
            # JSON roundtrip records actual values without retaining a UI object.
            result[name] = json.loads(json.dumps(value.value, default=str))
    return result


def chart_context(spec):
    """Record plotted domains and hashes, never embed the underlying raw tables."""
    datasets, axes, filters = {}, [], []

    def walk(node, path="chart"):
        if isinstance(node, dict):
            for name, rows in node.get("datasets", {}).items():
                datasets[name] = rows
            data = node.get("data", {})
            if isinstance(data, dict) and "url" in data:
                raise ValueError("Publication figures must embed captured data; external chart URLs lack a plotted-data fingerprint")
            if isinstance(data, dict) and isinstance(data.get("values"), list):
                datasets[path + ".data"] = data["values"]
            for channel, encoding in node.get("encoding", {}).items():
                if isinstance(encoding, dict):
                    axes.append({"channel": channel, **{k: encoding[k] for k in ("field", "type", "title", "axis", "scale") if k in encoding}})
            filters.extend(transform["filter"] for transform in node.get("transform", []) if "filter" in transform)
            for key, value in node.items():
                if key not in {"datasets", "data"}:
                    walk(value, path + "." + key)
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, path + f"[{index}]")

    walk(spec)
    domains = {}
    for rows in datasets.values():
        for row in rows:
            if not isinstance(row, dict):
                continue
            for field, value in row.items():
                key = field.lower().replace(" ", "_")
                if ("year" in key or "unit" in key or "vintage" in key or key in {
                    "region", "vehicle_group", "vehicle_class", "atb_class", "powertrain", "powertrain_family",
                    "profile", "source", "series", "currency", "target_currency", "source_currency", "metric",
                }) and value is not None:
                    domains.setdefault(field, set()).add(json.dumps(value, sort_keys=True, default=str))
    return {
        "title": spec.get("title"), "axes": axes, "filters": filters, "parameters": spec.get("params", []),
        "plotted_domains": {field: [json.loads(value) for value in sorted(values)] for field, values in sorted(domains.items())},
        "data_fingerprints": {name: {"rows": len(rows), "sha256": digest_json(rows)} for name, rows in datasets.items()},
        "vega_lite_sha256": digest_json(spec),
        "domain_note": "Year fields retain their source names: model vintages, observation years, projection years and dollar years are not interchangeable. Data are the chart's input rows; any Vega-Lite filters are recorded separately.",
    }


def notebook_evidence(name, definitions):
    if name == "stock_cohort_diagnosis":
        metadata = definitions["diagnosis"]["metadata"]
        paths = yaml.safe_load((ROOT / "config/paths.yaml").read_text(encoding="utf-8"))
        parameters = ROOT / paths["config"]["parameters"]
        configuration = [ROOT / "config/paths.yaml", ROOT / "config/sources.yaml",
                         parameters / "rules.yaml", parameters / "conversion.yaml", ROOT / metadata["scenario_path"]]
        return {"registered_input_recalculation": metadata,
                "configuration_fingerprints": {path.resolve().relative_to(ROOT).as_posix(): file_sha256(path) for path in configuration}}
    if name == "vehicle_population_diagnosis":
        return {"capture": definitions["evidence"]["build_metadata"]}
    if name == "ldv_efficiency_diagnosis":
        return {"diagnostic_summary": definitions["diagnostic"].summary,
                "display_settings": definitions["composition_view"]}
    raise ValueError(f"No explicit evidence identity reader for notebook: {name}")


def notebook_module(name):
    path = ROOT / "docs/insights" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, path


def export(groups):
    paths = yaml.safe_load((ROOT / "config/paths.yaml").read_text(encoding="utf-8"))["artifacts"]
    selections_path = ROOT / "config/parameters/documentation.yaml"
    settings = yaml.safe_load(selections_path.read_text(encoding="utf-8"))["publication_figures"]
    destination = ROOT / paths["documentation_assets"]["path"]
    destination.mkdir(parents=True, exist_ok=True)
    manifest_path = destination / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {"figures": {}}
    manifest.update(version=2, exported_at=datetime.now(UTC).isoformat(), selections_sha256=file_sha256(selections_path),
                    exporter_sha256=file_sha256(Path(__file__)),
                    revision=subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                                            capture_output=True, text=True).stdout.strip())
    seen = set()
    active_group = None
    (ROOT / "tmp").mkdir(exist_ok=True)
    staging = tempfile.TemporaryDirectory(prefix="documentation-figures-", dir=ROOT / "tmp")
    staging_path = Path(staging.name)

    def save(chart, generator, evidence, selections):
        data = chart.to_dict(validate=True)
        meta = data.get("usermeta", {})
        identity = meta.get("figure_id", "")
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", identity) or not meta.get("analytical_title"):
            raise ValueError(f"Chart lacks its notebook figure identity/title: {generator}")
        if identity in seen:
            raise ValueError(f"Duplicate exported figure identity: {identity}")
        previous = manifest["figures"].get(identity)
        if previous and previous["generator"] != generator:
            raise ValueError(f"Figure identity belongs to another notebook: {identity}")
        seen.add(identity)
        file = staging_path / f"{identity}.svg"
        context = chart_context(data)
        chart.save(file)
        manifest["figures"][identity] = {
            "file": file.name, "sha256": file_sha256(file), "title": meta["analytical_title"],
            "generator": generator, "generator_sha256": file_sha256(ROOT / generator),
            "group": active_group,
            "selections": selections, "evidence": evidence, "plotted_context": context,
            "exported_at": manifest["exported_at"], "exporter_sha256": manifest["exporter_sha256"],
            "selections_sha256": manifest["selections_sha256"],
        }
        print(identity, flush=True)

    def save_function(function, diagnostic, generator, evidence, selected_ids, **kwargs):
        bound = inspect.signature(function).bind(diagnostic, **kwargs)
        bound.apply_defaults()
        selections = {key: value for key, value in bound.arguments.items() if key != "diagnostic"}
        helper = Path(inspect.getsourcefile(function))
        chart = function(*bound.args, **bound.kwargs)
        if chart.usermeta["figure_id"] not in selected_ids:
            return
        save(chart, generator,
             {"capture": evidence, "chart_owner": {"path": helper.relative_to(ROOT).as_posix(), "sha256": file_sha256(helper)}},
             {"function": function.__name__, "arguments": selections, "selected_figure_ids": selected_ids})

    if "notebooks" in groups:
        active_group = "notebooks"
        import marimo as mo
        original = mo.ui.altair_chart
        for name, prefixes in settings["notebooks"].items():
            captured = {}

            def collect(chart, *args, **kwargs):
                meta = chart.usermeta
                if meta is not alt.Undefined:
                    identity = meta["figure_id"]
                    if any(matches(identity, p) for p in prefixes):
                        if identity in captured:
                            raise ValueError(f"Nonunique displayed figure identity: {identity}")
                        captured[identity] = chart
                return original(chart, *args, **kwargs)

            mo.ui.altair_chart = collect
            try:
                module, path = notebook_module(name)
                print(f"Executing {name}", flush=True)
                _, definitions = module.app.run()
            finally:
                mo.ui.altair_chart = original
            missing = [prefix for prefix in prefixes if not any(matches(identity, prefix) for identity in captured)]
            if missing:
                raise ValueError(f"Requested figures were not produced by {name}: {missing}; found {sorted(captured)}")
            evidence = notebook_evidence(name, definitions)
            local_helpers = set()
            for value in definitions.values():
                if inspect.isfunction(value) and (source := inspect.getsourcefile(value)):
                    source = Path(source).resolve()
                    if source.is_relative_to(ROOT / "src") and source.is_file():
                        local_helpers.add(source)
            evidence["local_callable_files"] = {path.relative_to(ROOT).as_posix(): file_sha256(path) for path in sorted(local_helpers)}
            controls = notebook_controls(definitions)
            for chart in captured.values():
                save(chart, path.relative_to(ROOT).as_posix(), evidence,
                     {"notebook_controls": controls, "selected_id_prefixes": prefixes,
                      "selection_note": "Actual selector values from the executed notebook; plotted domains below identify which values enter this figure."})

    if "cost" in groups:
        active_group = "cost"
        from validation.vehicle_cost_diagnostic import (load_diagnostic, source_price_chart,
            investment_trajectory_chart, maintenance_age_chart, service_maintenance_chart, range_cost_chart)
        diagnostic = load_diagnostic()
        s = settings["cost"]
        for function, arguments in [
            (source_price_chart, {"vehicle_class": s["vehicle_class"], "currency": s["currency"]}),
            (investment_trajectory_chart, {"region": s["region"], "mode": s["mode"]}),
            (maintenance_age_chart, {"vehicle_class": s["vehicle_class"], "source_year": s["source_year"], "currency": s["currency"]}),
            (service_maintenance_chart, {"region": s["region"], "mode": s["mode"], "vintage": s["vintage"]}),
            (range_cost_chart, {"region": s["region"], "mode": s["mode"]}),
        ]:
            save_function(function, diagnostic, "docs/insights/vehicle_cost_diagnosis.py", diagnostic.metadata, s["figure_ids"], **arguments)
        if missing := set(s["figure_ids"]) - seen:
            raise ValueError(f"Requested cost figures were not produced: {sorted(missing)}")

    if "calibration" in groups:
        active_group = "calibration"
        from validation.ceud_calibration_diagnostic import load_calibration_diagnostic, calibration_charts
        diagnostic = load_calibration_diagnostic()
        for chart in calibration_charts(diagnostic, **settings["calibration"]).values():
            helper = Path(inspect.getsourcefile(calibration_charts))
            save(chart, "docs/insights/ceud_calibration_diagnosis.py",
                 {"capture": diagnostic.metadata, "chart_owner": {"path": helper.relative_to(ROOT).as_posix(), "sha256": file_sha256(helper)}},
                 settings["calibration"])

    if "fleet" in groups:
        active_group = "fleet"
        from validation.documentation_support import load_evidence, fleet_100_chart
        evidence = load_evidence()
        save(fleet_100_chart(evidence), "docs/insights/documentation_support.py", evidence.metadata,
             evidence.settings.model_dump(mode="json"))
        table = staging_path / evidence.settings.exact_table
        evidence.shares.to_csv(table, index=False)
        manifest["figures"][evidence.settings.figure_id]["exact_table"] = {
            "file": table.name, "sha256": file_sha256(table), "rows": len(evidence.shares),
        }

    if "nonroad" in groups:
        active_group = "nonroad"
        from validation.documentation_support import load_nonroad_evidence, nonroad_efficiency_chart
        evidence = load_nonroad_evidence()
        save(nonroad_efficiency_chart(evidence), "docs/insights/documentation_support.py", evidence.metadata,
             evidence.settings.model_dump(mode="json"))
        table = staging_path / evidence.settings.exact_table
        evidence.rows.to_csv(table, index=False)
        manifest["figures"][evidence.settings.figure_id]["exact_table"] = {
            "file": table.name, "sha256": file_sha256(table), "rows": len(evidence.rows),
        }

    if "comparison" in groups:
        active_group = "comparison"
        s = settings["comparison"]
        module, path = notebook_module(s["notebook"])
        _, definitions = module.app.run()
        for name in s["charts"]:
            chart = definitions[name]
            expected = "comparison-" + name.replace("_chart", "").replace("_", "-")
            if chart.usermeta is alt.Undefined or chart.usermeta.get("figure_id") != expected:
                raise ValueError(f"Comparison notebook figure identity is missing/different: {name}; expected {expected}")
            evidence = {key: value for key, value in definitions["diagnosis"].items() if key.endswith("metadata")}
            save(chart, path.relative_to(ROOT).as_posix(), evidence,
                 {"notebook_controls": notebook_controls(definitions), "chart": name})

    # Publish only after every requested group succeeds. Remove only stale files
    # whose previous manifest explicitly assigned them to these notebooks.
    for identity, previous in list(manifest["figures"].items()):
        if previous.get("group") in groups and identity not in seen:
            stale = (destination / previous["file"]).resolve()
            if not stale.is_relative_to(destination.resolve()) or stale.suffix != ".svg":
                raise ValueError("Unsafe stale publication asset path")
            stale.unlink(missing_ok=True)
            del manifest["figures"][identity]
    for file in staging_path.iterdir():
        file.replace(destination / file.name)
    staged_manifest = staging_path / "manifest.json"
    staged_manifest.write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
    staged_manifest.replace(manifest_path)
    staging.cleanup()
    print(manifest_path, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--groups", nargs="+", choices=["notebooks", "cost", "calibration", "fleet", "nonroad", "comparison"],
                        default=["notebooks", "cost", "calibration", "fleet", "nonroad", "comparison"])
    export(parser.parse_args().groups)
