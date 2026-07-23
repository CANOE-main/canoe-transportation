from __future__ import annotations

import io
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
from openpyxl import Workbook

import fetching.nlr_atb_autonomie as adapter
from fetching.nlr_atb_autonomie import (
    NlrAtbAutonomieError,
    build_atb_request,
    build_manual_request,
    configured_trajectory,
    discover_zip_members,
    extract_bean_coefficients,
    fetch_archive_to_cache,
    module_rules,
    normalize_vehicles,
)
from utils import load_config_bundle


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


def _vehicle_frame() -> pd.DataFrame:
    metrics = [
        "Fuel Economy (mi/dge)",
        "Fuel Economy (mi/gge)",
        "Modeled Vehicle Price (2022$)",
        "Range (mi)",
        "Useable Energy (kWh)",
    ]
    rows = []
    for trajectory in ["Advanced", "Conservative", "Constant", "Mid"]:
        for index, metric in enumerate(metrics, start=1):
            rows.append(
                {
                    "year": 2030,
                    "scenario": trajectory,
                    "vehicle_weight_category": "Light Duty",
                    "vehicle_class": "Compact",
                    "vehicle_powertrain": "Battery Electric",
                    "vehicle_detail": "Battery Electric Vehicle (150-mile range)",
                    "fuel_category": "Electricity",
                    "fuel2_category": None,
                    "metric": metric,
                    "value": float(index),
                    "reference": "fixture",
                }
            )
    return pd.DataFrame(rows)


def _maintenance_ldv_bytes() -> bytes:
    sheets = {
        "class_multipliers": pd.DataFrame(
            [
                {
                    "vehicle_weight_category": "Light Duty",
                    "vehicle_class": "Compact",
                    "class_multiplier": 1.0,
                    "notes": "fixture",
                    "reference": "fixture",
                    "reference_url": "https://example.test/class",
                }
            ]
        ),
        "powertrain_multipliers": pd.DataFrame(
            [
                {
                    "vehicle_weight_category": "Light Duty",
                    "vehicle_powertrain": "Battery Electric",
                    "powertrain_multiplier": 0.67,
                    "notes": "fixture",
                    "reference": "fixture",
                    "reference_url": "https://example.test/powertrain",
                }
            ]
        ),
        "baseline_repair_cost": pd.DataFrame(
            [
                {
                    "vehicle_weight_category": "Light Duty",
                    "year_index": 0,
                    "baseline_repair_cost($/mi)": 0.0,
                    "dollar_year": 2020,
                    "notes": "fixture",
                    "reference": "fixture",
                    "reference_url": "https://example.test/repair",
                }
            ]
        ),
        "maintenance_cost": pd.DataFrame(
            [
                {
                    "vehicle_weight_category": "Light Duty",
                    "vehicle_powertrain": "Battery Electric",
                    "maintenance_cost($/mi)": 0.061,
                    "dollar_year": 2020,
                    "notes": "fixture",
                    "reference": "fixture",
                    "reference_url": "https://example.test/maintenance",
                }
            ]
        ),
    }
    content = io.BytesIO()
    with pd.ExcelWriter(content, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
    return content.getvalue()


def _write_fixture_zip(path: Path, *, prefix: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(path, "w") as archive:
        archive.writestr(
            f"{prefix}output/vehicles.csv",
            _vehicle_frame().to_csv(index=False),
        )
        archive.writestr(
            f"{prefix}input/maintenance_ldv.xlsx",
            _maintenance_ldv_bytes(),
        )
        archive.writestr(
            f"{prefix}input/vmt_ldv.csv",
            pd.DataFrame(
                [
                    {
                        "vehicle_weight_category": "Light Duty",
                        "vehicle_class": "Compact",
                        "vehicle_class_NHTSA": "Car",
                        "year_index": 0,
                        "vmt(mi)": 15_922,
                        "reference": "fixture",
                        "notes": "fixture",
                    }
                ]
            ).to_csv(index=False),
        )
        archive.writestr(
            f"{prefix}input/vmt_mdhd.csv",
            pd.DataFrame(
                [
                    {
                        "vehicle_weight_category": "Medium/Heavy Duty",
                        "vehicle_class": "Class 8 Sleeper",
                        "notes": "fixture",
                        "reference": "fixture",
                        "0": 100_000,
                        "1": 90_000,
                    }
                ]
            ).to_csv(index=False),
        )


def test_source_contract_and_conservative_legacy_default(bundle) -> None:
    source = bundle.sources.sources[adapter.ATB_SOURCE_ID]
    anl_source = bundle.sources.sources[adapter.ANL_SOURCE_ID]
    rules = module_rules(bundle)

    assert configured_trajectory(bundle) == "Conservative"
    assert source.component("vehicles").parameter_modules == [
        "efficiencies",
        "capex_opex",
    ]
    assert source.component("vmt_ldv").parameter_modules == ["stocks_and_demands"]
    assert set(build_atb_request(bundle).expected_trajectories) == {
        "Advanced",
        "Conservative",
        "Constant",
        "Mid",
    }
    assert set(anl_source.adapter) == {"access", "external_subdir", "expected_workbook"}
    assert rules["components"]["anl_bean"]["workbook_range"] == "A33:H54"
    assert rules["components"]["anl_bean"]["table_layout"]["header_row"] == 33


def test_zip_member_discovery_accepts_release_prefix_and_rejects_ambiguity(
    bundle, tmp_path: Path
) -> None:
    request = build_atb_request(bundle)
    archive_path = tmp_path / "atb.zip"
    _write_fixture_zip(archive_path, prefix="release/")

    members = discover_zip_members(archive_path, request.components)

    assert members["vehicles"] == "release/output/vehicles.csv"
    with ZipFile(archive_path, "a") as archive:
        archive.writestr("output/vehicles.csv", _vehicle_frame().to_csv(index=False))
    with pytest.raises(NlrAtbAutonomieError, match="vehicles is ambiguous"):
        discover_zip_members(archive_path, request.components)


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        assert chunk_size > 0
        yield self.payload


class _Session:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls = 0

    def get(self, url: str, *, stream: bool, timeout: int):
        assert url.startswith("https://")
        assert stream is True
        assert timeout > 0
        self.calls += 1
        return _Response(self.payload)


def test_cache_download_is_atomic_and_reused(bundle, tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.zip"
    _write_fixture_zip(fixture)
    request = build_atb_request(bundle).model_copy(
        update={"cache_path": tmp_path / "cache" / "atb.zip"}
    )
    session = _Session(fixture.read_bytes())

    assert fetch_archive_to_cache(request, session=session) == "downloaded"
    assert fetch_archive_to_cache(request, session=session) == "cached"
    assert session.calls == 1
    assert not request.cache_path.with_suffix(".zip.part").exists()


def test_required_columns_and_all_trajectories_are_retained(bundle) -> None:
    request = build_atb_request(bundle)
    component = next(item for item in request.components if item.component_id == "vehicles")
    frame = _vehicle_frame()

    normalized = normalize_vehicles(
        frame,
        request=request,
        component=component,
        source_member="output/vehicles.csv",
        default_trajectory="Conservative",
        rules=module_rules(bundle),
    )

    assert set(normalized["trajectory"]) == {
        "Advanced",
        "Conservative",
        "Constant",
        "Mid",
    }
    assert normalized.loc[normalized["is_default_trajectory"], "trajectory"].eq(
        "Conservative"
    ).all()
    with pytest.raises(NlrAtbAutonomieError, match="missing required columns"):
        normalize_vehicles(
            frame.drop(columns="metric"),
            request=request,
            component=component,
            source_member="output/vehicles.csv",
            default_trajectory="Conservative",
            rules=module_rules(bundle),
        )


def test_configured_anl_workbook_range_is_extracted_to_long_coefficients(
    bundle, tmp_path: Path
) -> None:
    configured = build_manual_request(bundle)
    workbook_path = tmp_path / "bean.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = configured.sheet_name
    layout = configured.table_layout
    for column, vehicle_class in layout["vehicle_class_columns"].items():
        sheet[f"{column}{layout['header_row']}"] = vehicle_class
    for block_index, block in enumerate(layout["blocks"].values(), start=1):
        first_row, last_row = block["rows"]
        sheet[f"{layout['label_column']}{first_row}"] = block["source_label"]
        for row, powertrain in zip(
            range(first_row, last_row + 1),
            layout["expected_powertrains"],
            strict=True,
        ):
            sheet[f"{layout['powertrain_column']}{row}"] = powertrain
            for column_index, column in enumerate(
                layout["vehicle_class_columns"], start=1
            ):
                sheet[f"{column}{row}"] = block_index + column_index / 10
    workbook.save(workbook_path)
    request = configured.model_copy(update={"workbook_path": workbook_path})

    extracted = extract_bean_coefficients(request)

    assert len(extracted) == 126
    assert set(extracted["coefficient_key"]) == {
        "maintenance_powertrain_multiplier",
        "coefficient_a",
        "coefficient_b",
    }
    assert set(extracted["vehicle_powertrain"]) == set(layout["expected_powertrains"])
    assert set(extracted["vehicle_class"]) == set(
        layout["vehicle_class_columns"].values()
    )
    assert extracted.loc[0, "source_row"] == 34
    assert extracted.loc[0, "source_column"] == "C"


def test_full_fixture_run_writes_manifest_outputs_and_manual_warning(
    bundle, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_bundle = replace(bundle, repo_root=tmp_path)
    rules = module_rules(bundle)
    request = build_atb_request(local_bundle)
    _write_fixture_zip(request.cache_path)
    monkeypatch.setattr(adapter, "load_config_bundle", lambda _: local_bundle)
    monkeypatch.setattr(adapter, "module_rules", lambda _: rules)

    output_dir = adapter.fetch_and_normalize("ignored.yaml", download=False)

    manifest = pd.read_csv(output_dir / rules["manifest_file"])
    vehicles = pd.read_csv(
        output_dir / rules["components"]["vehicles"]["output_file"]
    )
    assert set(manifest["component_id"]) == {
        "vehicles",
        "maintenance_ldv",
        "vmt_ldv",
        "vmt_mdhd",
        "mhdv_maintenance_coefficients",
    }
    assert manifest.loc[
        manifest["component_id"].eq("mhdv_maintenance_coefficients"), "status"
    ].item() == "warning"
    assert set(vehicles["trajectory"]) == {
        "Advanced",
        "Conservative",
        "Constant",
        "Mid",
    }
    warning_text = (output_dir / rules["warnings_file"]).read_text(encoding="utf-8")
    assert "Download the complete Box folder" in warning_text


def test_missing_cached_and_manual_artifacts_fail_clearly(bundle, tmp_path: Path) -> None:
    request = build_atb_request(bundle)
    with pytest.raises(FileNotFoundError):
        discover_zip_members(tmp_path / "missing.zip", request.components)

    manual = build_manual_request(bundle).model_copy(
        update={"workbook_path": tmp_path / "missing.xlsm", "required": True}
    )
    with pytest.raises(FileNotFoundError, match="Required manual ANL workbook missing"):
        extract_bean_coefficients(manual)
