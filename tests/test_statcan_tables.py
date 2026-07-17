from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

from fetching.statcan_tables import (
    StatCanSourceError,
    _select_chunk,
    build_freight_candidates,
    build_ldv_history,
    build_requests,
    ensure_cached_artifacts,
    module_rules,
    normalize_table,
    read_selected_table,
    resolve_download_url,
    scenario_regions,
    validate_metadata_contract,
    write_outputs,
)
from utils import load_config_bundle, load_conversion_factors


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


@pytest.fixture
def rules(bundle):
    return module_rules(bundle)


def request_for(bundle, table_id: str):
    return next(request for request in build_requests(bundle) if request.table_id == table_id)


def test_scenario_region_resolution_uses_quoted_config_and_geography_map(bundle, rules) -> None:
    assert scenario_regions(bundle) == ["ON"]
    assert rules["geography"]["ON"] == {
        "label": "Ontario",
        "freight_contains": ["Ontario"],
    }
    with pytest.raises(StatCanSourceError, match="No StatCan geography mapping"):
        scenario_regions(bundle, ["XX"])


def test_request_resolution_uses_wds_and_configured_cache_root(bundle) -> None:
    request = request_for(bundle, "20-10-0025-01")

    assert request.product_id == 20100025
    assert request.download_api_url.endswith(
        "/getFullTableDownloadCSV/20100025/en"
    )
    assert request.archive_cache_path == (
        REPO_ROOT / "inputs" / "0_cache" / "statcan_transport" / "20100025-eng.zip"
    )
    assert request.metadata_cache_path.name == "20100025-metadata.json"


def test_download_url_resolution_validates_wds_response(bundle) -> None:
    request = request_for(bundle, "20-10-0021-01")

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {
                "status": "SUCCESS",
                "object": "https://www150.statcan.gc.ca/n1/tbl/csv/20100021-eng.zip",
            }

    class Session:
        def get(self, url: str, *, timeout: int):
            assert url == request.download_api_url
            assert timeout == 60
            return Response()

    assert resolve_download_url(request, session=Session()) == (
        "https://www150.statcan.gc.ca/n1/tbl/csv/20100021-eng.zip"
    )


def test_offline_cache_reuse_and_missing_cache_failure(bundle, tmp_path: Path) -> None:
    request = replace(
        request_for(bundle, "20-10-0021-01"),
        metadata_cache_path=tmp_path / "metadata.json",
        archive_cache_path=tmp_path / "table.zip",
    )
    with pytest.raises(FileNotFoundError, match="Offline StatCan cache is incomplete"):
        ensure_cached_artifacts(request, download=False)

    request.metadata_cache_path.write_text("{}", encoding="utf-8")
    request.archive_cache_path.write_bytes(b"cached")
    assert ensure_cached_artifacts(request, download=False) == (
        "cached",
        "cached",
        "",
    )


def test_metadata_contract_rejects_changed_dimensions_and_members(bundle) -> None:
    request = request_for(bundle, "23-10-0308-01")
    metadata = {
        "productId": str(request.product_id),
        "cubeTitleEn": request.title,
        "dimension": [
            {
                "dimensionNameEn": "Geography",
                "member": [{"memberNameEn": "Ontario"}],
            }
        ],
    }

    with pytest.raises(StatCanSourceError, match="Required dimensions changed"):
        validate_metadata_contract(metadata, request)


def test_exact_province_filter_excludes_subprovincial_geographies(bundle, rules) -> None:
    request = request_for(bundle, "20-10-0025-01")
    base = {
        "Fuel type": "Gasoline",
        "Vehicle type": "Passenger cars",
        "Statistics": "Number of vehicles",
    }
    frame = pd.DataFrame(
        [
            {"GEO": "Ontario", **base},
            {"GEO": "Toronto, Ontario", **base},
            {"GEO": "Alberta", **base},
        ]
    )

    selected = _select_chunk(
        frame,
        request=request,
        regions=["ON"],
        geography_rules=rules["geography"],
    )

    assert len(selected) == 1
    assert selected[0]["GEO"].tolist() == ["Ontario"]
    assert selected[0]["scenario_region"].tolist() == ["ON"]


def test_common_normalization_preserves_statcan_provenance(bundle) -> None:
    request = request_for(bundle, "34-10-0254-01")
    raw = pd.DataFrame(
        [
            {
                "REF_DATE": "2020",
                "GEO": "Ontario",
                "DGUID": "2021A000235",
                "Public transit assets, average expected useful life": (
                    "Diesel buses, average expected useful life"
                ),
                "UOM": "Years",
                "UOM_ID": 379,
                "SCALAR_FACTOR": "units",
                "SCALAR_ID": 0,
                "VECTOR": "v1",
                "COORDINATE": "7.1",
                "VALUE": 14.5,
                "STATUS": "",
                "SYMBOL": "",
                "TERMINATED": "",
                "DECIMALS": 1,
                "scenario_region": "ON",
            }
        ]
    )

    normalized = normalize_table(
        raw,
        request=request,
        source_member="34100254-eng.csv",
        scalar_multipliers={"units": 1.0},
    )

    assert normalized.loc[0, "table_id"] == "34-10-0254-01"
    assert normalized.loc[0, "scenario_region"] == "ON"
    assert normalized.loc[0, "reference_period"] == "2020"
    assert normalized.loc[0, "units"] == "Years"
    assert normalized.loc[0, "scalar_factor"] == "units"
    assert normalized.loc[0, "scaled_value"] == 14.5
    assert normalized.loc[0, "vector"] == "v1"


def _ldv_rows(period_values: list[tuple[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "scenario_region": "ON",
                "geography": "Ontario",
                "fuel_type": "Gasoline",
                "vehicle_type": "Passenger cars",
                "statistics": "Number of vehicles",
                "units": "Number",
                "reference_period": period,
                "scaled_value": value,
            }
            for period, value in period_values
        ]
    )


def test_ldv_overlap_policy_prefers_complete_current_years_and_warns(rules) -> None:
    archived = _ldv_rows([("2016", 10.0), ("2017", 20.0)])
    current = _ldv_rows(
        [
            ("2017-01", 6.0),
            ("2017-04", 6.0),
            ("2017-07", 6.0),
            ("2017-10", 6.0),
            ("2018-01", 7.0),
        ]
    )
    warnings: list[str] = []

    history, overlap = build_ldv_history(
        archived,
        current,
        rules=rules,
        warnings=warnings,
    )

    assert history[["reference_year", "scaled_value"]].values.tolist() == [
        [2016, 10.0],
        [2017, 24.0],
    ]
    assert history["source_table_id"].tolist() == [
        "20-10-0021-01",
        "20-10-0025-01",
    ]
    assert overlap.loc[0, "difference_current_minus_archived"] == 4.0
    assert any("excluded" in warning for warning in warnings)
    assert any("take precedence" in warning for warning in warnings)


def test_freight_origin_or_destination_filter_is_region_driven(bundle, rules) -> None:
    request = request_for(bundle, "23-10-0142-01")
    base = {
        "Mode of transportation": "Truck (for-hire)",
        "Commodity group": "Food",
        "Characteristics": "Shipments",
    }
    frame = pd.DataFrame(
        [
            {
                "GEO": "Toronto, Ontario, origin of shipments",
                "Geography, destination of shipments": "Quebec, destination of shipments",
                **base,
            },
            {
                "GEO": "Alberta, origin of shipments",
                "Geography, destination of shipments": "Hamilton, Ontario, destination of shipments",
                **base,
            },
            {
                "GEO": "Alberta, origin of shipments",
                "Geography, destination of shipments": "Manitoba, destination of shipments",
                **base,
            },
        ]
    )

    selected = _select_chunk(
        frame,
        request=request,
        regions=["ON"],
        geography_rules=rules["geography"],
    )
    result = pd.concat(selected, ignore_index=True)

    assert result["region_match"].tolist() == ["origin", "destination"]
    assert result["scenario_region"].unique().tolist() == ["ON"]


def _freight_long_rows(
    *,
    group: str,
    shipments: float,
    average_weight_kg: float,
    average_distance_km: float,
    tonne_km: float = 1000.0,
) -> list[dict[str, object]]:
    values = {
        "Shipments": shipments,
        "Weight": shipments * average_weight_kg,
        "Distance": shipments * average_distance_km,
        "Tonne-kilometres": tonne_km,
    }
    return [
        {
            "table_id": "23-10-0142-01",
            "product_id": 23100142,
            "scenario_region": "ON",
            "region_match": "origin",
            "reference_period": "2017",
            "geography_origin_of_shipments": group,
            "geography_destination_of_shipments": "Quebec",
            "mode_of_transportation": "Truck (for-hire)",
            "commodity_group": "Food",
            "characteristics": characteristic,
            "scaled_value": value,
        }
        for characteristic, value in values.items()
    ]


def test_freight_curb_weight_threshold_and_exact_350_mile_boundary(rules, bundle) -> None:
    mile_to_km = float(load_conversion_factors(bundle)["length"]["mile_to_km"])
    boundary_km = 350.0 * mile_to_km
    rows = _freight_long_rows(
        group="exact",
        shipments=10,
        average_weight_kg=1970.0,
        average_distance_km=boundary_km,
    )
    rows += _freight_long_rows(
        group="long",
        shipments=10,
        average_weight_kg=2000.0,
        average_distance_km=boundary_km + 1,
    )
    rows += _freight_long_rows(
        group="below_weight",
        shipments=10,
        average_weight_kg=1969.0,
        average_distance_km=100,
    )

    result = build_freight_candidates(
        pd.DataFrame(rows),
        rules=rules,
        mile_to_km=mile_to_km,
        warnings=[],
    )

    by_origin = result.set_index("geography_origin_of_shipments")
    assert set(by_origin.index) == {"exact", "long"}
    assert by_origin.loc["exact", "gross_vehicle_weight_kg"] == 14970.0
    assert by_origin.loc["exact", "haul_class"] == "regional"
    assert by_origin.loc["long", "haul_class"] == "long_haul"
    assert result["table_id"].unique().tolist() == ["23-10-0142-01"]


def test_freight_haul_distribution_rule_uses_tonne_kilometres(rules, bundle) -> None:
    mile_to_km = float(load_conversion_factors(bundle)["length"]["mile_to_km"])
    rows = _freight_long_rows(
        group="regional_a",
        shipments=10,
        average_weight_kg=2000.0,
        average_distance_km=100.0,
        tonne_km=100.0,
    )
    rows += _freight_long_rows(
        group="regional_b",
        shipments=10,
        average_weight_kg=2000.0,
        average_distance_km=200.0,
        tonne_km=100.0,
    )
    rows += _freight_long_rows(
        group="long",
        shipments=10,
        average_weight_kg=2000.0,
        average_distance_km=600.0,
        tonne_km=800.0,
    )

    result = build_freight_candidates(
        pd.DataFrame(rows),
        rules=rules,
        mile_to_km=mile_to_km,
        warnings=[],
    )
    distribution = rules["freight"]["haul_distribution"]
    tonne_km_by_class = result.groupby(distribution["category_column"])[
        distribution["weight_column"]
    ].sum()
    shares = tonne_km_by_class / tonne_km_by_class.sum()

    assert distribution["aggregation"] == "sum"
    assert distribution["normalization"] == "share_of_total_tonne_kilometres"
    assert shares.to_dict() == {"long_haul": 0.8, "regional": 0.2}
    assert result["haul_class"].value_counts(normalize=True)["regional"] == 2 / 3

    row_count_rules = {
        **rules,
        "freight": {
            **rules["freight"],
            "haul_distribution": {
                **distribution,
                "weight_column": "row_count",
            },
        },
    }
    with pytest.raises(StatCanSourceError, match="incompatible rules"):
        build_freight_candidates(
            pd.DataFrame(rows),
            rules=row_count_rules,
            mile_to_km=mile_to_km,
            warnings=[],
        )


def test_manifest_and_warning_outputs_are_written(rules, tmp_path: Path) -> None:
    normalized = {"20-10-0021-01": pd.DataFrame([{"value": 1}])}

    write_outputs(
        normalized_tables=normalized,
        history=pd.DataFrame([{"reference_year": 2016}]),
        overlap=pd.DataFrame([{"reference_year": 2017}]),
        freight_candidates=pd.DataFrame([{"haul_class": "regional"}]),
        manifest_rows=[{"table_id": "20-10-0021-01", "status": "ok"}],
        warnings=["overlap differs"],
        output_dir=tmp_path,
        rules=rules,
    )

    manifest = pd.read_csv(tmp_path / rules["manifest_file"])
    assert manifest.loc[0, "status"] == "ok"
    assert (tmp_path / rules["warnings_file"]).read_text(encoding="utf-8") == (
        "overlap differs\n"
    )
    assert (tmp_path / rules["ldv_history"]["output_file"]).exists()
    assert (tmp_path / rules["freight"]["output_file"]).exists()


def test_changed_csv_dimensions_fail_with_available_columns(bundle, rules, tmp_path: Path) -> None:
    request = replace(
        request_for(bundle, "20-10-0021-01"),
        archive_cache_path=tmp_path / "20100021-eng.zip",
    )
    csv_path = tmp_path / "20100021.csv"
    csv_path.write_text("REF_DATE,GEO,VALUE\n2011,Ontario,1\n", encoding="utf-8")
    with ZipFile(request.archive_cache_path, "w") as archive:
        archive.write(csv_path, arcname=csv_path.name)

    with pytest.raises(StatCanSourceError, match="Required CSV columns changed"):
        read_selected_table(
            request,
            regions=["ON"],
            geography_rules=rules["geography"],
            chunksize=10,
        )


def test_cached_metadata_shape_can_be_serialized_for_offline_fixture(bundle) -> None:
    request = request_for(bundle, "20-10-0021-01")
    payload = [{"status": "SUCCESS", "object": {"productId": str(request.product_id)}}]
    assert json.loads(json.dumps(payload))[0]["object"]["productId"] == "20100021"
