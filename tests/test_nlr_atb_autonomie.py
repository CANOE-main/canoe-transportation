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
    derive_phev_efficiency,
    discover_zip_members,
    extract_bean_coefficients,
    fetch_archive_to_cache,
    match_phev_utility_factors,
    module_rules,
    normalize_vehicles,
)
from utils import load_config_bundle, load_conversion_factors


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
    rows.extend(
        [
            {
                "year": 2030,
                "scenario": "Conservative",
                "vehicle_weight_category": "Light Duty",
                "vehicle_class": "Compact",
                "vehicle_powertrain": "Plug-in Hybrid",
                "vehicle_detail": "Gasoline PHEV",
                "fuel_category": "Electricity",
                "fuel2_category": "Gasoline",
                "metric": "Fuel Economy (mi/gge)",
                "value": 70.0,
                "reference": "fixture output evidence",
            },
            {
                "year": 2030,
                "scenario": "Mid",
                "vehicle_weight_category": "Medium/Heavy Duty",
                "vehicle_class": "Class 8 Sleeper",
                "vehicle_powertrain": "Plug-in Hybrid",
                "vehicle_detail": "Diesel PHEV",
                "fuel_category": "Electricity",
                "fuel2_category": "Diesel",
                "metric": "Fuel Economy (mi/dge)",
                "value": 30.0,
                "reference": "fixture output evidence",
            },
            {
                "year": 2030,
                "scenario": "Conservative",
                "vehicle_weight_category": "Light Duty",
                "vehicle_class": "Compact",
                "vehicle_powertrain": "Plug-in Hybrid",
                "vehicle_detail": "Gasoline PHEV",
                "fuel_category": "Electricity",
                "fuel2_category": "Gasoline",
                "metric": "Modeled Vehicle Price (2022$)",
                "value": 40_000.0,
                "reference": "fixture",
            },
        ]
    )
    return pd.DataFrame(rows)


def _phev_vehicle_inputs() -> pd.DataFrame:
    base = {
        "fuel_category": "Electricity",
        "vehicle_powertrain": "Plug-in Hybrid",
        "battery_cost($)": 0.0,
        "pack_energy(kWh)": 0.0,
        "usable_energy(kWh)": 0.0,
        "fuel_cell_cost($/kW)": 0.0,
        "h2_storage_tank_cost($/kgh2)": 0.0,
        "vehicle_cost($)": 0.0,
        "dollar_year": 2022,
        "reference": "fixture vehicle inputs",
    }
    return pd.DataFrame(
        [
            {
                **base,
                "year": 2030,
                "scenario": "Constant",
                "fuel2_category": "Gasoline",
                "vehicle_weight_category": "Light Duty",
                "vehicle_class": "Compact",
                "vehicle_detail": "Gasoline PHEV",
                "CS(mi/gge)": 40.0,
                "CD(Wh/mi)": 300.0,
                "ARB_contribution": pd.NA,
                "EPA55_contribution": pd.NA,
                "EPA65_contribution": pd.NA,
                "ARB_CS(mpgde)": pd.NA,
                "EPA55_CS(mpgde)": pd.NA,
                "EPA65_CS(mpgde)": pd.NA,
                "ARB_CD(Wh/mi)": pd.NA,
                "EPA55_CD(Wh/mi)": pd.NA,
                "EPA65_CD(Wh/mi)": pd.NA,
                "range(mi)": 35.0,
            },
            {
                **base,
                "year": 2030,
                "scenario": "Mid",
                "fuel2_category": "Diesel",
                "vehicle_weight_category": "Medium/Heavy Duty",
                "vehicle_class": "Class 8 Sleeper",
                "vehicle_detail": "Diesel PHEV",
                "CS(mi/gge)": pd.NA,
                "CD(Wh/mi)": pd.NA,
                "ARB_contribution": 0.2,
                "EPA55_contribution": 0.3,
                "EPA65_contribution": 0.5,
                "ARB_CS(mpgde)": 10.0,
                "EPA55_CS(mpgde)": 20.0,
                "EPA65_CS(mpgde)": 40.0,
                "ARB_CD(Wh/mi)": 1000.0,
                "EPA55_CD(Wh/mi)": 2000.0,
                "EPA65_CD(Wh/mi)": 3000.0,
                "range(mi)": 100.0,
            },
        ]
    )


def _phev_uf_ldv() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "vehicle_weight_category": "Light Duty",
                "range(mi)": 30.0,
                "phev_uf": 0.5,
                "reference": "fixture LDV UF",
                "notes": "lower",
            },
            {
                "vehicle_weight_category": "Light Duty",
                "range(mi)": 40.0,
                "phev_uf": 0.6,
                "reference": "fixture LDV UF",
                "notes": "upper",
            },
        ]
    )


def _phev_uf_mdhd() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "vehicle_weight_category": "Medium/Heavy Duty",
                "vehicle_class": "Class 8 Sleeper",
                "phev_uf": 0.8,
                "reference": "fixture MHDV UF",
                "notes": "exact",
            }
        ]
    )


def _derived_phev(bundle) -> pd.DataFrame:
    rules = module_rules(bundle)["components"]["phev_efficiency"]
    return derive_phev_efficiency(
        _phev_vehicle_inputs(),
        ldv_utility_factors=_phev_uf_ldv(),
        mdhd_utility_factors=_phev_uf_mdhd(),
        output_vehicles=_vehicle_frame(),
        rules=rules,
        conversions=load_conversion_factors(bundle),
        source_members={
            "phev_vehicle_inputs": "input/inputs_vehicles.csv",
            "phev_utility_factor_ldv": "input/phev_uf_ldv.csv",
            "phev_utility_factor_mdhd": "input/phev_uf_mdhd.csv",
            "vehicles": "output/vehicles.csv",
        },
        default_trajectory="Conservative",
    )


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
            f"{prefix}input/inputs_vehicles.csv",
            _phev_vehicle_inputs().to_csv(index=False),
        )
        archive.writestr(
            f"{prefix}input/phev_uf_ldv.csv",
            _phev_uf_ldv().to_csv(index=False),
        )
        archive.writestr(
            f"{prefix}input/phev_uf_mdhd.csv",
            _phev_uf_mdhd().to_csv(index=False),
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
    assert {
        "phev_vehicle_inputs",
        "phev_utility_factor_ldv",
        "phev_utility_factor_mdhd",
    }.issubset(source.components)
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

    nested_archive = tmp_path / "nested.zip"
    _write_fixture_zip(nested_archive, prefix="release/nested/")
    with pytest.raises(NlrAtbAutonomieError, match="missing required component vehicles"):
        discover_zip_members(nested_archive, request.components)


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
    assert len(
        normalized[
            normalized["vehicle_powertrain"].eq("Battery Electric")
            & normalized["metric"].str.startswith("Fuel Economy")
        ]
    ) == 8
    assert normalized[
        normalized["vehicle_powertrain"].eq("Plug-in Hybrid")
    ]["metric"].tolist() == ["Modeled Vehicle Price (2022$)"]
    with pytest.raises(NlrAtbAutonomieError, match="missing required columns"):
        normalize_vehicles(
            frame.drop(columns="metric"),
            request=request,
            component=component,
            source_member="output/vehicles.csv",
            default_trajectory="Conservative",
            rules=module_rules(bundle),
        )


def test_ldv_phev_uses_combined_inputs_interpolated_uf_and_energy_shares(bundle) -> None:
    derived = _derived_phev(bundle)
    row = derived[derived["vehicle_weight_category"].eq("Light Duty")].iloc[0]
    conversion = load_conversion_factors(bundle)

    assert row["fuel_equivalent_basis"] == "gge"
    assert row["fleet_utility_factor"] == pytest.approx(0.55)
    assert row["utility_factor_match_method"] == "linear_interpolation"
    assert row["utility_factor_lower_range_mi"] == 30
    assert row["utility_factor_upper_range_mi"] == 40
    assert row["combined_cs_fuel_economy_mi_per_gallon_equivalent"] == 40
    assert row["combined_cd_electricity_consumption_wh_per_mi"] == 300
    assert row[
        "utility_weighted_fuel_consumption_gallon_equivalent_per_mi"
    ] == pytest.approx(0.45 / 40)
    assert row[
        "utility_weighted_electricity_consumption_wh_per_mi"
    ] == pytest.approx(0.55 * 300)
    assert row[
        "utility_weighted_fuel_consumption_litre_equivalent_per_100_km"
    ] == pytest.approx(
        (0.45 / 40)
        * conversion["derived"][
            "us_gallon_equivalent_per_mile_to_litre_equivalent_per_100_km"
        ]
    )
    assert row["utility_weighted_fuel_consumption_canadian_unit"] == "Lge/100 km"
    assert row["electricity_input_share"] != pytest.approx(
        row["fleet_utility_factor"]
    )
    assert row["electricity_input_share"] + row["liquid_fuel_input_share"] == (
        pytest.approx(1.0)
    )


def test_mhdv_phev_uses_harmonic_cs_arithmetic_cd_and_dge_basis(bundle) -> None:
    row = _derived_phev(bundle).loc[
        lambda frame: frame["vehicle_weight_category"].eq("Medium/Heavy Duty")
    ].iloc[0]
    expected_cs = 1.0 / (0.2 / 10.0 + 0.3 / 20.0 + 0.5 / 40.0)
    expected_cd = 0.2 * 1000.0 + 0.3 * 2000.0 + 0.5 * 3000.0

    assert row["fuel_equivalent_basis"] == "dge"
    assert row["combined_cs_fuel_economy_mi_per_gallon_equivalent"] == pytest.approx(
        expected_cs
    )
    assert row["combined_cd_electricity_consumption_wh_per_mi"] == pytest.approx(
        expected_cd
    )
    assert row["source_cycle_contribution_sum"] == pytest.approx(1.0)
    assert row["source_arb_cycle_contribution"] == pytest.approx(0.2)
    assert row["utility_factor_match_method"] == "exact"
    assert row["utility_weighted_fuel_consumption_canadian_unit"] == "Lde/100 km"
    assert row["wh_per_fuel_equivalent_gallon"] == pytest.approx(
        load_conversion_factors(bundle)["energy"]["wh_per_dge"]
    )


def test_utility_factor_bounds_ambiguity_missing_and_no_extrapolation(bundle) -> None:
    rules = module_rules(bundle)["components"]["phev_efficiency"]
    vehicle = _derived_phev(bundle).loc[
        lambda frame: frame["vehicle_weight_category"].eq("Light Duty"),
        ["vehicle_weight_category", "vehicle_class", "electric_range_mi"],
    ]
    bad_bounds = _phev_uf_ldv()
    bad_bounds.loc[0, "phev_uf"] = 1.1
    with pytest.raises(NlrAtbAutonomieError, match=r"must lie in \[0, 1\]"):
        match_phev_utility_factors(
            vehicle,
            ldv_utility_factors=bad_bounds,
            mdhd_utility_factors=_phev_uf_mdhd(),
            rules=rules,
        )

    ambiguous = pd.concat([_phev_uf_ldv(), _phev_uf_ldv().iloc[[0]]])
    with pytest.raises(NlrAtbAutonomieError, match="ambiguous duplicate keys"):
        match_phev_utility_factors(
            vehicle,
            ldv_utility_factors=ambiguous,
            mdhd_utility_factors=_phev_uf_mdhd(),
            rules=rules,
        )

    extrapolated = vehicle.copy()
    extrapolated["electric_range_mi"] = 20
    with pytest.raises(NlrAtbAutonomieError, match="would extrapolate"):
        match_phev_utility_factors(
            extrapolated,
            ldv_utility_factors=_phev_uf_ldv(),
            mdhd_utility_factors=_phev_uf_mdhd(),
            rules=rules,
        )

    missing_mdhd = _phev_uf_mdhd().iloc[0:0]
    mdhd_vehicle = pd.DataFrame(
        [
            {
                "vehicle_weight_category": "Medium/Heavy Duty",
                "vehicle_class": "Class 8 Sleeper",
                "electric_range_mi": 100,
            }
        ]
    )
    with pytest.raises(NlrAtbAutonomieError, match="contains no rows"):
        match_phev_utility_factors(
            mdhd_vehicle,
            ldv_utility_factors=_phev_uf_ldv(),
            mdhd_utility_factors=missing_mdhd,
            rules=rules,
        )


def test_phev_canadian_conversions_round_trip_without_rounding(bundle) -> None:
    derived = _derived_phev(bundle)
    factors = load_conversion_factors(bundle)["derived"]
    fuel_factor = factors[
        "us_gallon_equivalent_per_mile_to_litre_equivalent_per_100_km"
    ]
    electricity_factor = factors["wh_per_mile_to_kwh_per_100_km"]

    assert (
        derived[
            "utility_weighted_fuel_consumption_litre_equivalent_per_100_km"
        ]
        / fuel_factor
    ).to_numpy() == pytest.approx(
        derived[
            "utility_weighted_fuel_consumption_gallon_equivalent_per_mi"
        ].to_numpy()
    )
    assert (
        derived[
            "utility_weighted_electricity_consumption_kwh_per_100_km"
        ]
        / electricity_factor
    ).to_numpy() == pytest.approx(
        derived["utility_weighted_electricity_consumption_wh_per_mi"].to_numpy()
    )
    assert derived["electricity_input_share"].between(0, 1).all()
    assert derived["liquid_fuel_input_share"].between(0, 1).all()


def test_phev_reconciliation_is_diagnostic_and_uses_scenario_alias(bundle) -> None:
    derived = _derived_phev(bundle)
    ldv = derived[derived["vehicle_weight_category"].eq("Light Duty")].iloc[0]

    assert ldv["trajectory"] == "Constant"
    assert ldv["reconciliation_output_scenario"] == "Conservative"
    assert ldv["reconciliation_match_method"] == "scenario_alias"
    assert not bool(ldv["reconciliation_within_tolerance"])
    assert (
        ldv["combined_cs_fuel_economy_mi_per_gallon_equivalent"]
        == _phev_vehicle_inputs().iloc[0]["CS(mi/gge)"]
    )


def test_invalid_mhdv_cycle_contributions_fail(bundle) -> None:
    inputs = _phev_vehicle_inputs()
    inputs.loc[1, "EPA65_contribution"] = 0.4
    rules = module_rules(bundle)["components"]["phev_efficiency"]
    with pytest.raises(NlrAtbAutonomieError, match="must sum to 1"):
        derive_phev_efficiency(
            inputs,
            ldv_utility_factors=_phev_uf_ldv(),
            mdhd_utility_factors=_phev_uf_mdhd(),
            output_vehicles=_vehicle_frame(),
            rules=rules,
            conversions=load_conversion_factors(bundle),
            source_members={
                "phev_vehicle_inputs": "input/inputs_vehicles.csv",
                "phev_utility_factor_ldv": "input/phev_uf_ldv.csv",
                "phev_utility_factor_mdhd": "input/phev_uf_mdhd.csv",
                "vehicles": "output/vehicles.csv",
            },
            default_trajectory="Conservative",
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
    monkeypatch.setattr(
        adapter,
        "load_conversion_factors",
        lambda _: load_conversion_factors(bundle),
    )

    output_dir = adapter.fetch_and_normalize("ignored.yaml", download=False)

    manifest = pd.read_csv(output_dir / rules["manifest_file"])
    vehicles = pd.read_csv(
        output_dir / rules["components"]["vehicles"]["output_file"]
    )
    assert set(manifest["component_id"]) == {
        "vehicles",
        "maintenance_ldv",
        "phev_vehicle_inputs",
        "phev_utility_factor_ldv",
        "phev_utility_factor_mdhd",
        "phev_efficiency_derivation",
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
    phev = pd.read_csv(
        output_dir / rules["components"]["phev_efficiency"]["output_file"]
    )
    phev_bytes = (
        output_dir / rules["components"]["phev_efficiency"]["output_file"]
    ).read_bytes()
    assert len(phev) == 2
    assert set(phev["fuel_equivalent_basis"]) == {"gge", "dge"}
    assert phev["reconciliation_output_fuel_economy_mi_per_gallon_equivalent"].notna().all()
    assert adapter.fetch_and_normalize("ignored.yaml", download=False) == output_dir
    assert (
        output_dir / rules["components"]["phev_efficiency"]["output_file"]
    ).read_bytes() == phev_bytes
    warning_text = (output_dir / rules["warnings_file"]).read_text(encoding="utf-8")
    assert "Download the complete Box folder" in warning_text
    assert "PHEV output fuel-economy reconciliation is report-only" in warning_text


def test_missing_cached_and_manual_artifacts_fail_clearly(bundle, tmp_path: Path) -> None:
    request = build_atb_request(bundle)
    with pytest.raises(FileNotFoundError):
        discover_zip_members(tmp_path / "missing.zip", request.components)

    manual = build_manual_request(bundle).model_copy(
        update={"workbook_path": tmp_path / "missing.xlsm", "required": True}
    )
    with pytest.raises(FileNotFoundError, match="Required manual ANL workbook missing"):
        extract_bean_coefficients(manual)
