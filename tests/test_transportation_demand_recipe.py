"""Workbook semantics require independent source levels and calendar evidence."""

from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree
from zipfile import ZipFile

import pandas as pd
import pytest

from fetching import cer_enerfuture
from utils import file_sha256
from validation.transportation_research import deepen_demand_diagnosis
from test_transportation_research import demand_diagnosis_fixture


def set_growth_formula(path: Path, numerator_column="C", denominator_row=2):
    """Retain cached values while supplying the formulas Excel stores beside them."""
    with ZipFile(path) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    tree = ElementTree.fromstring(contents["xl/worksheets/sheet1.xml"])
    for row in tree.findall(f"{namespace}sheetData/{namespace}row"):
        index = int(row.attrib["r"])
        if index > 1:
            cell = next(item for item in row if item.attrib["r"] == f"B{index}")
            formula = ElementTree.Element(f"{namespace}f")
            formula.text = f"{numerator_column}{index}/{numerator_column}${denominator_row}"
            cell.insert(0, formula)
    contents["xl/worksheets/sheet1.xml"] = ElementTree.tostring(tree)
    with ZipFile(path, "w") as archive:
        for name, content in contents.items():
            archive.writestr(name, content)


def test_formula_is_evaluated_separately_from_cached_growth_and_label(tmp_path, monkeypatch):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    path = tmp_path / "demand_growth_rate.xlsx"
    set_growth_formula(path)
    result["fleet_metadata"]["notebook"]["dependencies"][1]["provided_sha256"] = file_sha256(path)
    frames = deepen_demand_diagnosis(result, None, rules)
    audit = frames["demand_growth_series_audit"].set_index("calendar_year")
    assert audit.loc[2030, "workbook_growth_formula"] == "=C4/C$2"
    assert audit.loc[2030, "model_label"] == 2025
    assert audit.loc[2030, "compiler_emits_year"]
    baseline = frames["demand_baseline_service_targets"].iloc[0]
    assert baseline.legacy_calendar_base_year_demand_bn == 11.
    assert baseline.legacy_calendar_base_year_model_label == 2020
    assert not baseline.legacy_calendar_base_year_emitted
    assert baseline.current_base_year_demand_bn == 10.5
    assert baseline.historical_calibration_target_bn == 12.


@pytest.mark.parametrize("defect", ["wrong_formula", "wrong_formula_anchor"])
def test_stale_cache_or_shifted_formula_anchor_fails(tmp_path, monkeypatch, defect):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    path = tmp_path / "demand_growth_rate.xlsx"
    set_growth_formula(path, numerator_column="A" if defect == "wrong_formula" else "C",
                       denominator_row=3 if defect == "wrong_formula_anchor" else 2)
    result["fleet_metadata"]["notebook"]["dependencies"][1]["provided_sha256"] = file_sha256(path)
    with pytest.raises(ValueError, match="cached growth index"):
        deepen_demand_diagnosis(result, None, rules)


@pytest.mark.parametrize("wrong_source_level", [False, True])
def test_economic_identity_is_checked_against_independent_cer_levels(tmp_path, monkeypatch, wrong_source_level):
    result, rules = demand_diagnosis_fixture(tmp_path, monkeypatch)
    path = tmp_path / "cer_macro.csv"
    macro = pd.DataFrame([
        {"scenario": "Current Measures", "year": year, "variable_key": key, "value": value}
        for year, deflator, gdp in [(2023, 100., 1000.), (2025, 110., 1050.),
                                    (2030, 120., 1500.), (2035, 150., 1800.)]
        for key, value in (("gdp_deflator", deflator), ("real_gdp", gdp))
    ])
    if wrong_source_level:
        macro.loc[macro.year.eq(2030) & macro.variable_key.eq("gdp_deflator"), "value"] = 121.
    macro.to_csv(path, index=False)
    request = SimpleNamespace(component_id="macro-indicators", cache_path=path, edition=2026,
                              url="https://example.test/registered-cer.csv")
    monkeypatch.setattr(cer_enerfuture, "module_rules", lambda bundle: {})
    monkeypatch.setattr(cer_enerfuture, "build_requests", lambda bundle: [request])
    monkeypatch.setattr(cer_enerfuture, "read_and_validate_source", lambda selected: pd.read_csv(selected.cache_path))
    monkeypatch.setattr(cer_enerfuture, "normalize_component", lambda raw, *args, **kwargs: raw)
    control = SimpleNamespace(scenario=SimpleNamespace(demand=SimpleNamespace(cer_scenario="Current Measures")))
    if wrong_source_level:
        with pytest.raises(ValueError, match="deflator levels conflict"):
            deepen_demand_diagnosis(result, control, rules)
    else:
        frames = deepen_demand_diagnosis(result, control, rules)
        audit = frames["demand_growth_series_audit"].set_index("calendar_year")
        assert audit.loc[2030, "registered_deflator_index"] == 1.2
        assert audit.loc[2030, "registered_real_gdp_index"] == 1.5
        assert audit.loc[2030, "growth_index_difference"] == pytest.approx(-.3)
        assert audit.loc[2030, "source_sha256"] == file_sha256(path)
        assert "price deflator" in audit.loc[2030, "compiler_note_conflict"]
