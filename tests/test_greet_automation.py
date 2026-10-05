"""Control coverage, source-specific convergence and isolated Excel supervision."""

import json
from pathlib import Path
import subprocess

import openpyxl
import pandas as pd
from pydantic import ValidationError
import pytest

from fetching.greet_automation import (
    audit_lightweight_glider, probe_archetypes, probe_cases,
    generate_vehicle_cycle_results, inspect_lightweight_glider_formulas, validate_probe_results,
)
from fetching.greet_vehicle_cycle import GenerationSettings, registered_workbooks
from utils import load_config_bundle, load_harmonization_rules


ROOT = Path(__file__).resolve().parents[1]


def test_glider_formula_audit_surfaces_shifted_ranges_and_exact_proposal():
    settings = registered_workbooks(load_config_bundle("config/scenarios/legacy_reproduction.yaml", repo_root=ROOT))[0].automation
    original = "=IF(GREET1_Import_Export!$G$12=200,W4,IF(GREET1_Import_Export!$G$12=300,Y4,IF(GREET1_Import_Export!$G$12=400,AA4,AC4)))+(J17+IF(Vehi_Inputs!$C$76=1,J18,J19))"
    expected = original.replace("=200,W4", "=150,W4").replace("=300,Y4", "=200,Y4").replace("=400,AA4", "=300,AA4")
    cells = {"J4": original, "W3": "EV150: Lightweight Material", "Y3": "EV200: Lightweight Material",
             "AA3": "EV300: Lightweight Material", "AC3": "EV400: Lightweight Material"}
    audit = audit_lightweight_glider(cells, settings.lightweight_glider)
    assert not audit["ok"]
    assert not audit["correction_applied"]
    assert audit["proposed_formula"] == expected
    assert cells["J4"] == original
    assert [(c["selected_aer_miles"], c["referenced_glider_aer_miles"]) for c in audit["range_checks"]] == [(150, 400), (200, 150), (300, 200), (400, 300)]
    cells["J4"] = expected
    assert audit_lightweight_glider(cells, settings.lightweight_glider)["ok"]


@pytest.mark.parametrize("value", [2383.14, 2500.0, None, "missing", True, float("inf")])
def test_saved_glider_inputs_are_audited_without_correcting_formula(tmp_path, value):
    spec = registered_workbooks(load_config_bundle("config/scenarios/legacy_reproduction.yaml", repo_root=ROOT))[0].automation.lightweight_glider
    path = tmp_path / "greet2.xlsx"
    book = openpyxl.Workbook()
    book.remove(book.active)
    original = "=IF(GREET1_Import_Export!$G$12=200,W4,IF(GREET1_Import_Export!$G$12=300,Y4,IF(GREET1_Import_Export!$G$12=400,AA4,AC4)))+(J17+IF(Vehi_Inputs!$C$76=1,J18,J19))"
    for name in spec.worksheets:
        sheet = book.create_sheet(name)
        sheet["J4"] = original
        for column, miles in (("W", 150), ("Y", 200), ("AA", 300), ("AC", 400)):
            sheet[f"{column}3"] = f"EV{miles}: Lightweight Material"
            sheet[f"{column}4"] = 2383.14
        sheet["Y4"] = value
    book.save(path)
    book.close()
    before = path.read_bytes()
    if value is None or isinstance(value, (str, bool)) or value == float("inf"):
        with pytest.raises(ValueError, match="Missing or invalid GREET lightweight glider input"):
            inspect_lightweight_glider_formulas(path, spec)
    else:
        audits = inspect_lightweight_glider_formulas(path, spec)
        assert all(not a["ok"] and not a["correction_applied"] for a in audits)
        assert all(a["original_formula"] == original for a in audits)
        assert all(a["glider_inputs_equal"] == (value == 2383.14) for a in audits)
        assert all(a["glider_input_values"]["Y4"] == value for a in audits)
    assert path.read_bytes() == before


@pytest.fixture
def inputs():
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml", repo_root=ROOT)
    rules = load_harmonization_rules(bundle, "greet_vehicle_cycle")
    settings = registered_workbooks(bundle)[0].automation
    ranges, _ = probe_archetypes(bundle)
    cases = probe_cases(rules, ranges)
    records = []
    for case in cases:
        for family in ("ldv", "mhdv"):
            for block in rules[family]["blocks"]:
                for gas, multiplier in (("CO2", 1), ("CH4", .01), ("N2O", .001)):
                    base = case[family + "_selector"] * 1e6
                    if family == "ldv" and block["powertrain"] in ("phev", "bev"):
                        base += case[block["powertrain"] + "_range_miles"] * 1000
                    if block.get("materials") == "lightweight":
                        base *= .8
                    records.append({**case, "family": family, "vehicle_class": case[family + "_class"],
                                    "powertrain": block["powertrain"], "materials": block.get("materials", "none"),
                                    "gas_label": gas, "source_value": base * multiplier,
                                    "source_units": "g/vehicle-lifetime"})
    return bundle, rules, settings, cases, pd.DataFrame(records)


def test_probe_covers_current_atb_ranges_without_year_expansion(inputs):
    bundle, rules, _, cases, _ = inputs
    ranges, path = probe_archetypes(bundle)
    assert path.is_file()
    assert set(ranges["phev"].values()) == {35, 50}
    assert set(ranges["bev"].values()) == {150, 200, 300, 400}
    assert len(cases) == 23
    assert {c["ldv_class"] for c in cases} == set(rules["ldv_selectors"])
    assert {c["mhdv_class"] for c in cases} == set(rules["mhdv_selectors"])
    assert sum(c["repeat"] for c in cases) == 4
    assert all("year" not in c for c in cases)


def test_probe_validates_independent_material_and_gas_results(inputs):
    _, rules, settings, cases, frame = inputs
    report = validate_probe_results(frame, settings, cases=cases, rules=rules)
    assert report["ok"]
    assert report["individual_gas_rows"] == 966
    assert len(report["range_response"]) == 12
    assert len(report["mhdv_classes"]) == 3
    assert report["independence_comparisons"] > 0


@pytest.mark.parametrize("problem", ["missing", "duplicate", "cross_control", "stale_range", "combined_co2", "unsaved", "units"])
def test_probe_rejects_incomplete_stale_or_cross_dependent_outputs(inputs, problem):
    _, rules, settings, cases, frame = inputs
    if problem == "missing":
        frame = frame.iloc[1:]
    elif problem == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif problem == "cross_control":
        index = frame.loc[frame.family.eq("mhdv") & frame.case_id.eq(1)].index[0]
        frame.loc[index, "source_value"] += 100
    elif problem == "stale_range":
        frame.loc[frame.powertrain.eq("bev"), "source_value"] = 1000
    elif problem == "combined_co2":
        frame.loc[0, "gas_label"] = "CO2 (VOC, CO, CO2)"
    elif problem == "unsaved":
        frame.loc[0, "source_value"] = float("nan")
    else:
        frame.loc[0, "source_units"] = "g/mile"
    with pytest.raises(ValueError):
        validate_probe_results(frame, settings, cases=cases, rules=rules)


@pytest.mark.parametrize("filename", ["../source.xlsm", "..", "owned_excel.json", "execution.log"])
def test_probe_rejects_unsafe_output_names_before_io(inputs, filename):
    payload = inputs[2].model_dump()
    payload["files"]["results"] = filename
    with pytest.raises(ValidationError, match="relative filenames"):
        GenerationSettings.model_validate(payload)


def test_probe_timeout_terminates_only_the_owned_excel_pid(inputs, tmp_path, monkeypatch):
    bundle = inputs[0]
    killed = []
    monkeypatch.setattr("fetching.greet_automation.resolve_artifact_path", lambda *_: tmp_path)
    monkeypatch.setattr("fetching.greet_automation.os.kill", lambda pid, _: killed.append(pid))

    def timeout(command, **kwargs):
        run = Path(command[-1])
        (run / "owned_excel.json").write_text(json.dumps({"pid": 12345}))
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr("fetching.greet_automation.subprocess.run", timeout)
    with pytest.raises(subprocess.TimeoutExpired):
        generate_vehicle_cycle_results(bundle)
    assert killed == [12345]
    report = json.loads(next(tmp_path.glob("*/run_report.json")).read_text())
    assert not report["ok"]
    assert report["error"] == "GREET probe timeout"
