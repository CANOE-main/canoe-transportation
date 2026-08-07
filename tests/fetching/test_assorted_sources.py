from dataclasses import replace
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
from openpyxl import Workbook
from pydantic import ValidationError

import fetching.assorted_sources as adapter
from fetching.assorted_sources import (
    ArtifactRequest,
    AssortedSourcesError,
    discover_regen_payload_request,
    fetch_to_cache,
    normalize_gcam,
    normalize_nems,
    normalize_nhtsa,
    normalize_regen,
    parse_faa_table_text,
)
from utils import load_config_bundle, resolve_input_path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


@pytest.fixture
def rules(bundle):
    return adapter.module_rules(bundle)


def _request(
    path: Path,
    file_type: str,
    *,
    source_id: str = "fixture_source",
    component_id: str = "fixture_component",
    url: str = "https://example.test/source",
) -> ArtifactRequest:
    content = path.read_bytes()
    return ArtifactRequest(
        source_id=source_id,
        component_id=component_id,
        source_version="fixture-version",
        url=url,
        cache_path=path.resolve(),
        file_type=file_type,
        expected_sha256=sha256(content).hexdigest(),
        expected_bytes=len(content),
    )


def _nhtsa_zip(
    path: Path,
    rules: dict[str, object],
    *,
    member: str | None = None,
    sheet_name: str | None = None,
    bad_age: bool = False,
    bad_value: bool = False,
) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = sheet_name or str(rules["worksheet"])
    for coordinate, value in rules["title_headers"].items():
        sheet[str(coordinate)] = value
    for column, label in rules["class_columns"].items():
        sheet[f"{column}{rules['class_header_row']}"] = label
    start = int(rules["data_start_row"])
    ages = range(
        int(rules["expected_ages"]["start"]),
        int(rules["expected_ages"]["end"]) + 1,
    )
    for offset, age in enumerate(ages):
        sheet[f"A{start + offset}"] = age + 1 if bad_age and offset == 0 else age
        for column_index, column in enumerate(rules["class_columns"], start=1):
            value = max(0.0, 1.0 - age / 50.0 - column_index / 1000.0)
            sheet[f"{column}{start + offset}"] = (
                None if bad_value and offset == 2 and column == "B" else value
            )
    workbook_bytes = BytesIO()
    workbook.save(workbook_bytes)
    with ZipFile(path, "w") as archive:
        archive.writestr(member or str(rules["workbook_member"]), workbook_bytes.getvalue())


def _nems_workbook(
    path: Path,
    rules: dict[str, object],
    *,
    sheet_name: str | None = None,
    bad_header: bool = False,
    bad_age: bool = False,
    bad_value: bool = False,
) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = sheet_name or str(rules["worksheet"])
    sheet["A86"] = "changed" if bad_header else str(rules["age_header"])
    for column, label in rules["class_columns"].items():
        sheet[f"{column}86"] = label
    ages = range(
        int(rules["expected_ages"]["start"]),
        int(rules["expected_ages"]["end"]) + 1,
    )
    for offset, age in enumerate(ages, start=87):
        sheet[f"A{offset}"] = age + 1 if bad_age and offset == 87 else age
        for column_index, column in enumerate(rules["class_columns"], start=1):
            value = min(0.95, age / 500.0 + column_index / 1000.0)
            sheet[f"{column}{offset}"] = (
                1.2 if bad_value and offset == 90 and column == "B" else value
            )
    workbook.save(path)


def _gcam_csv(
    path: Path,
    rules: dict[str, object],
    *,
    drop_column: str | None = None,
    changed_filter: bool = False,
    changed_unit: bool = False,
    null_value: bool = False,
    duplicate: bool = False,
) -> None:
    rows: list[dict[str, object]] = []
    years = [int(year) for year in rules["expected_years"]]
    for technology, fuel in (("Liquids", "Liquids"), ("BEV", "Electricity")):
        for variable in rules["variables"]:
            row = {
                "UCD_region": "Canada",
                "UCD_sector": "Passenger",
                "mode": "LDV_2W",
                "size.class": (
                    "changed"
                    if changed_filter
                    else rules["filters"]["size.class"]
                ),
                "UCD_technology": technology,
                "UCD_fuel": fuel,
                "variable": variable,
                "unit": (
                    "changed"
                    if changed_unit and variable == "intensity"
                    else rules["variable_units"][variable]
                ),
            }
            row.update({str(year): float(index + 1) for index, year in enumerate(years)})
            if null_value and technology == "BEV" and variable == "intensity":
                row[str(years[0])] = None
            rows.append(row)
    rows.extend(
        [
            {
                **rows[0],
                "variable": "Capital costs (infrastructure)",
                "unit": "2005$/veh",
            },
            {
                **rows[0],
                "UCD_region": "USA",
            },
        ]
    )
    if duplicate:
        rows.append(dict(rows[0]))
    frame = pd.DataFrame(rows)
    if drop_column:
        frame = frame.drop(columns=[drop_column])
    csv = frame.to_csv(index=False, lineterminator="\n")
    path.write_text(
        "# File: fixture.csv\n# Units: Various\n# ----------\n" + csv,
        encoding="utf-8",
        newline="\n",
    )


def _regen_payload(
    rules: dict[str, object],
    *,
    gasoline_null: bool = True,
    diesel_null: bool = False,
    changed_title: bool = False,
    changed_measure: bool = False,
    changed_technology: bool = False,
) -> str:
    assignments: list[str] = []
    calls: list[str] = []
    for chart_number, names in (
        ("3", ("A", "B", "C", "D")),
        ("4", ("E", "F", "G", "H")),
    ):
        chart = rules["charts"][chart_number]
        data_name, measures_name, keys_name, fields_name = names
        technologies = dict(chart["expected_technologies"])
        if changed_technology and chart_number == "4":
            technologies["icev-mgs"] = "changed"
        keys = [
            {
                "Field": rules["dimension_field"],
                "Key": key,
                "Name": label,
                "Description": label,
                "Subtype": None,
            }
            for key, label in technologies.items()
        ]
        data = []
        for technology_index, technology in enumerate(technologies):
            for year_index, year in enumerate(chart["expected_years"]):
                value: float | None = float(
                    100 + technology_index * 10 + year_index
                )
                if (
                    gasoline_null
                    and chart_number == "4"
                    and technology == "icev-mgs"
                ):
                    value = None
                if (
                    diesel_null
                    and chart_number == "4"
                    and technology == "icev-dsl"
                ):
                    value = None
                data.append(
                    {
                        "tech": technology,
                        "vintage": int(year),
                        "bus_int": value,
                        "other": 999.0,
                    }
                )
        measure_title = (
            "Changed"
            if changed_measure and chart_number == "4"
            else rules["measure_title"]
        )
        measures = {
            "bus_int": {
                "title": measure_title,
                "description": "Intercity bus",
                "dataUnit": chart["expected_unit"],
                "displayUnit": chart["expected_unit"],
                "precision": 3,
            },
            "other": {
                "title": "Other measure",
                "description": None,
                "dataUnit": chart["expected_unit"],
                "displayUnit": chart["expected_unit"],
                "precision": 3,
            },
        }
        assignments.extend(
            [
                f"{data_name}={json.dumps(data, separators=(',', ':'))}",
                f"{measures_name}={json.dumps(measures, separators=(',', ':'))}",
                f"{keys_name}={json.dumps(keys, separators=(',', ':'))}",
                f"{fields_name}={{dimensions:{{}},measures:{measures_name}}}",
            ]
        )
        title = (
            f"{chart['title']} changed"
            if changed_title and chart_number == "4"
            else chart["title"]
        )
        calls.append(
            f'render({{id:"{chart["element_id"]}",caption:"{title}",'
            f'chartType:"line",dataSource:{data_name},fieldSource:{fields_name},'
            f'keySource:{keys_name},dimensionField:"{rules["dimension_field"]}",'
            f'categoryField:"{rules["category_field"]}",valueField:"ldf_car"}})'
        )
    return ";" + ",".join(assignments) + ";" + ";".join(calls)


def _faa_texts(rules: dict[str, object]) -> tuple[str, str]:
    tables = rules["tables"]
    section_3 = f"""
    {rules["documents"]["section_3_capacity"]["document_title"]}
    {tables["3-6"]["title"]}
    Aircraft Category Passenger Capacity Passenger Load Factor Average Block Speed (MPH)
    All Aircraft 19,879,016 7,984,009 169 84% 24 64% 6 366
    Sources: fixture
    {tables["3-9"]["title"]}
    Aircraft Category Cargo Capacity Cargo Load Factor Average Block Speed (MPH)
    All Aircraft 2,142,095 733,310 93 48% 2 399
    Sources: fixture
    """
    section_4 = f"""
    {rules["documents"]["section_4_operating_costs"]["document_title"]}
    {tables["4-7"]["title"]}
    Aircraft Category Cost per Block Hour Maintenance Total Variable Block Hours
    All Aircraft $2,760 $1,005 $1,396 $5,161 $362 $256 $6 $13 $638 $5,799 19,582,623
    Sources: fixture
    {tables["4-8"]["title"]}
    Aircraft Category Cost per Block Hour Maintenance Total Variable Block Hours
    All Aircraft $5,574 $2,815 $2,470 $10,859 $1,034 $484 $28 $349 $1,895 $12,754 1,927,078
    Sources: fixture
    """
    return section_3, section_4


def test_artifact_request_and_invalid_download_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="URL is invalid"):
        ArtifactRequest(
            source_id="source",
            component_id="component",
            source_version="version",
            url="moving-main",
            cache_path=(tmp_path / "file.csv").resolve(),
            file_type="csv",
            expected_sha256="0" * 64,
            expected_bytes=1,
        )

    target = tmp_path / "bad.csv"
    request = ArtifactRequest(
        source_id="source",
        component_id="component",
        source_version="version",
        url="https://example.test/file.csv",
        cache_path=target.resolve(),
        file_type="csv",
        expected_sha256="0" * 64,
        expected_bytes=10,
    )

    class Response:
        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size: int):
            assert chunk_size > 0
            yield b"short"

    class Session:
        def get(self, url: str, *, stream: bool, timeout: int):
            assert url == request.url
            assert stream and timeout > 0
            return Response()

    with pytest.raises(AssortedSourcesError, match="byte count changed"):
        fetch_to_cache(request, session=Session())
    assert not target.exists()
    assert not target.with_suffix(".csv.part").exists()


def test_nhtsa_fixture_normalizes_complete_long_form(
    rules, tmp_path: Path
) -> None:
    source_rules = rules["nhtsa_cafe"]
    path = tmp_path / "nhtsa.zip"
    _nhtsa_zip(path, source_rules)
    first = normalize_nhtsa(_request(path, "zip"), source_rules)
    second = normalize_nhtsa(_request(path, "zip"), source_rules)

    assert len(first) == 160
    assert set(first["source_vehicle_class_label"]) == set(
        source_rules["class_columns"].values()
    )
    assert list(first.groupby("source_vehicle_class_label")["vehicle_age"].count()) == [
        40,
        40,
        40,
        40,
    ]
    assert first["survival_rate"].between(0, 1).all()
    assert first.equals(second)
    assert set(first["archive_member"]) == {"parameters_ref.xlsx"}
    assert set(first["source_range"]) == {"A3:E45"}


@pytest.mark.parametrize(
    ("member", "sheet_name", "bad_age", "bad_value", "message"),
    [
        ("missing.xlsx", None, False, False, "expected one"),
        (None, "Changed", False, False, "missing worksheet"),
        (None, None, True, False, "age coverage changed"),
        (None, None, False, True, "is not numeric"),
    ],
)
def test_nhtsa_contract_failures(
    rules,
    tmp_path: Path,
    member: str | None,
    sheet_name: str | None,
    bad_age: bool,
    bad_value: bool,
    message: str,
) -> None:
    source_rules = rules["nhtsa_cafe"]
    path = tmp_path / "nhtsa.zip"
    _nhtsa_zip(
        path,
        source_rules,
        member=member,
        sheet_name=sheet_name,
        bad_age=bad_age,
        bad_value=bad_value,
    )
    with pytest.raises(AssortedSourcesError, match=message):
        normalize_nhtsa(_request(path, "zip"), source_rules)


def test_nems_fixture_stays_annual_scrappage_data(
    rules, tmp_path: Path
) -> None:
    source_rules = rules["eia_nems"]
    path = tmp_path / "nems.xlsx"
    _nems_workbook(path, source_rules)
    frame = normalize_nems(_request(path, "xlsx"), source_rules)

    assert len(frame) == 102
    assert "annual_scrappage_rate" in frame
    assert "survival" not in " ".join(frame.columns)
    assert set(frame["source_vehicle_class_label"]) == {"Cls 3", "Cls 4-6", "Cls 7-8"}
    assert frame["vehicle_age"].min() == 1
    assert frame["vehicle_age"].max() == 34
    assert frame["annual_scrappage_rate"].between(0, 1).all()


@pytest.mark.parametrize(
    ("sheet_name", "bad_header", "bad_age", "bad_value", "message"),
    [
        ("Changed", False, False, False, "missing worksheet"),
        (None, True, False, False, "age header changed"),
        (None, False, True, False, "age coverage changed"),
        (None, False, False, True, "is above"),
    ],
)
def test_nems_contract_failures(
    rules,
    tmp_path: Path,
    sheet_name: str | None,
    bad_header: bool,
    bad_age: bool,
    bad_value: bool,
    message: str,
) -> None:
    source_rules = rules["eia_nems"]
    path = tmp_path / "nems.xlsx"
    _nems_workbook(
        path,
        source_rules,
        sheet_name=sheet_name,
        bad_header=bad_header,
        bad_age=bad_age,
        bad_value=bad_value,
    )
    with pytest.raises(AssortedSourcesError, match=message):
        normalize_nems(_request(path, "xlsx"), source_rules)


def test_gcam_exact_filter_units_years_and_exclusions(
    rules, tmp_path: Path
) -> None:
    source_rules = rules["jgcri_gcam"]
    path = tmp_path / "gcam.csv"
    _gcam_csv(path, source_rules)
    frame, stats = normalize_gcam(_request(path, "csv"), source_rules)

    assert len(frame) == 120
    assert stats.input_records == 8
    assert stats.selected_records == 6
    assert stats.excluded_records == 2
    assert set(frame["source_technology"]) == {"BEV", "Liquids"}
    assert set(frame["source_variable"]) == set(source_rules["variables"])
    assert set(frame["source_unit"]) == set(source_rules["variable_units"].values())
    assert list(frame["source_year"].drop_duplicates().sort_values()) == list(
        source_rules["expected_years"]
    )
    assert not frame["source_variable"].str.contains(
        "infrastructure|insurance|toll", case=False
    ).any()
    assert frame.equals(normalize_gcam(_request(path, "csv"), source_rules)[0])


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"drop_column": "UCD_fuel"}, "missing required columns"),
        ({"changed_filter": True}, "selected no records"),
        ({"changed_unit": True}, "unit for"),
        ({"null_value": True}, "null/non-numeric"),
        ({"duplicate": True}, "duplicate keys"),
    ],
)
def test_gcam_contract_failures(
    rules, tmp_path: Path, kwargs: dict[str, bool | str], message: str
) -> None:
    source_rules = rules["jgcri_gcam"]
    path = tmp_path / "gcam.csv"
    _gcam_csv(path, source_rules, **kwargs)
    with pytest.raises(AssortedSourcesError, match=message):
        normalize_gcam(_request(path, "csv"), source_rules)


def test_regen_page_discovers_exact_configured_payload(
    bundle, rules, tmp_path: Path
) -> None:
    source_rules = rules["epri_us_regen"]
    source = bundle.sources.sources[source_rules["source_id"]]
    assert list(source.components) == [
        "intercity_bus_charts",
        "nonroad_cost_invest_multipliers",
        "nonroad_efficiency_multipliers",
    ]
    assert {
        "access",
        "cache_path",
        "expected_sha256",
        "expected_bytes",
        "payload_cache_path",
        "expected_payload_sha256",
        "expected_payload_bytes",
    } <= set(source.adapter)
    href = source_rules["payload_asset"]["href"]
    page = tmp_path / "page.html"
    page.write_text(
        "<!doctype html><title>"
        + source_rules["page_title"]
        + '</title><link rel="modulepreload" href="'
        + href
        + '"><link rel="modulepreload" href="/assets/transportation.html-meta.js">',
        encoding="utf-8",
    )
    request, identity = discover_regen_payload_request(
        bundle,
        _request(
            page,
            "html",
            source_id="epri_us_regen_2025_transportation",
            component_id=source_rules["component_id"],
            url="https://us-regen-docs.epri.com/v2025/assumptions/transportation.html",
        ),
        source_rules,
    )
    assert identity == href
    assert request.url.endswith(href)
    assert request.cache_path.name.endswith("_payload.js")

    page.write_text("<title>Changed</title>", encoding="utf-8")
    changed = _request(page, "html")
    with pytest.raises(AssortedSourcesError, match="page title changed"):
        discover_regen_payload_request(bundle, changed, source_rules)


def test_regen_nonroad_cost_invest_manual_table_matches_tables_4_and_6(
    bundle,
) -> None:
    source = bundle.sources.sources["epri_us_regen_2025_transportation"]
    component = source.component("nonroad_cost_invest_multipliers")
    path = resolve_input_path(
        bundle,
        "manual",
        component.adapter["manual_parameter_path"],
    )
    frame = pd.read_csv(path)

    assert list(frame.columns) == component.adapter["expected_columns"]
    assert not frame.duplicated(component.adapter["unique_key"]).any()
    frame = frame.loc[
        frame["source -> data_source"].eq(component.adapter["source_selector"])
    ].copy()
    assert len(frame) == component.adapter["expected_rows"] == 12
    assert frame["value"].gt(0).all()
    assert set(frame["technology_class"]) == {
        "passenger_rail",
        "freight_rail",
        "freight_marine",
    }
    assert not frame["powertrain"].str.startswith("electric").any()
    indexed = frame.set_index(["technology_class", "powertrain"])["value"]
    assert indexed["passenger_rail", "lng_2035"] == pytest.approx(1.03)
    assert indexed["passenger_rail", "h2_2050"] == pytest.approx(1.01)
    assert indexed["freight_rail", "lng_2035"] == pytest.approx(1.57)
    assert indexed["freight_rail", "h2_2050"] == pytest.approx(1.43)
    assert indexed["freight_marine", "h2_2035"] == pytest.approx(2.58)
    assert indexed["freight_marine", "h2_2050"] == pytest.approx(1.9)
    assert set(component.adapter["table_labels"]) == {"Table 4", "Table 6"}


def test_regen_structured_chart_extraction_and_currency_warning(
    rules, tmp_path: Path
) -> None:
    source_rules = rules["epri_us_regen"]
    page = tmp_path / "page.html"
    page.write_text("<html></html>", encoding="utf-8")
    payload = tmp_path / "payload.js"
    payload.write_text(_regen_payload(source_rules), encoding="utf-8")
    frame, warnings, stats = normalize_regen(
        _request(page, "html", component_id=source_rules["component_id"]),
        _request(payload, "javascript", component_id=source_rules["component_id"]),
        source_rules,
        payload_identity=source_rules["payload_asset"]["href"],
    )

    assert len(frame) == 76
    assert set(frame["chart_number"]) == {3, 4}
    assert set(frame["measure"]) == {"Intercity Bus"}
    assert set(frame.loc[frame["chart_number"].eq(3), "source_technology_label"]) == {
        "ICEV",
        "CNGV",
        "BEV",
        "HFCV",
    }
    assert set(frame.loc[frame["chart_number"].eq(4), "source_technology_label"]) == {
        "ICEV (dsl)",
        "CNGV",
        "BEV",
        "HFCV",
    }
    assert "ICEV (mgs)" not in set(frame["source_technology_label"])
    assert frame["currency_year"].isna().all()
    assert stats.selected_records == 76
    assert stats.excluded_records == 100
    assert [warning["code"] for warning in warnings] == [
        "regen_currency_year_unresolved",
        "regen_configured_empty_series_excluded",
    ]
    exclusion = warnings[1]
    assert exclusion["technology_label"] == "ICEV (mgs)"
    assert exclusion["excluded_records"] == 12
    assert exclusion["reason"] == source_rules["charts"]["4"][
        "excluded_empty_series"
    ]["icev-mgs"]["reason"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"gasoline_null": False},
            "configured empty-series exclusion.*contains a numeric value",
        ),
        ({"diesel_null": True}, "is not numeric"),
        ({"changed_title": True}, "chart identity"),
        ({"changed_measure": True}, "measure changed"),
        ({"changed_technology": True}, "technology metadata changed"),
    ],
)
def test_regen_chart_contract_failures(
    rules, tmp_path: Path, kwargs: dict[str, bool], message: str
) -> None:
    source_rules = rules["epri_us_regen"]
    page = tmp_path / "page.html"
    page.write_text("<html></html>", encoding="utf-8")
    payload = tmp_path / "payload.js"
    payload.write_text(_regen_payload(source_rules, **kwargs), encoding="utf-8")
    with pytest.raises(AssortedSourcesError, match=message):
        normalize_regen(
            _request(page, "html"),
            _request(payload, "javascript"),
            source_rules,
            payload_identity="fixture.js",
        )


def _faa_request(tmp_path: Path, component_id: str) -> ArtifactRequest:
    path = tmp_path / f"{component_id}.pdf"
    path.write_bytes(b"%PDF-fixture")
    return _request(
        path,
        "pdf",
        source_id="faa_economic_values_2024",
        component_id=component_id,
    )


def test_faa_tables_select_only_requested_metrics_and_preserve_raw_units(
    rules, tmp_path: Path
) -> None:
    source_rules = rules["faa"]
    section_3, section_4 = _faa_texts(source_rules)
    request_3 = _faa_request(tmp_path, "section_3_capacity")
    request_4 = _faa_request(tmp_path, "section_4_operating_costs")
    parsed = {}
    for table_id in ("3-6", "3-9"):
        parsed[table_id] = parse_faa_table_text(
            section_3,
            table_id=table_id,
            rules={**source_rules, **source_rules["tables"][table_id]},
            request=request_3,
            checksum=request_3.expected_sha256,
        )
    for table_id in ("4-7", "4-8"):
        parsed[table_id] = parse_faa_table_text(
            section_4,
            table_id=table_id,
            rules={**source_rules, **source_rules["tables"][table_id]},
            request=request_4,
            checksum=request_4.expected_sha256,
        )

    capacity = pd.concat([parsed["3-6"], parsed["3-9"]], ignore_index=True)
    maintenance = pd.concat([parsed["4-7"], parsed["4-8"]], ignore_index=True)
    assert set(capacity["operating_group"]) == {"passenger", "cargo"}
    assert set(capacity["metric"]) == {
        "passenger_capacity",
        "passenger_load_factor",
        "cargo_capacity",
        "cargo_load_factor",
        "average_block_speed",
    }
    cargo_capacity = capacity.loc[capacity["metric"].eq("cargo_capacity")].iloc[0]
    assert cargo_capacity["value"] == 93
    assert cargo_capacity["unit"] == "source-labelled tons"
    assert capacity.loc[
        capacity["metric"].str.endswith("load_factor"), "raw_value"
    ].str.endswith("%").all()
    assert set(maintenance["value"]) == {1005, 2815}
    assert 5161 not in set(maintenance["value"])
    assert set(maintenance["metric"]) == {"maintenance_cost_per_block_hour"}
    assert maintenance["currency_year"].isna().all()
    assert maintenance["raw_value"].str.startswith("$").all()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("title", "missing expected table title"),
        ("metric", "missing metric/header label"),
        ("row", "All Aircraft row is malformed"),
    ],
)
def test_faa_table_title_metric_and_multiline_row_failures(
    rules, tmp_path: Path, mutation: str, message: str
) -> None:
    source_rules = rules["faa"]
    section_3, _ = _faa_texts(source_rules)
    if mutation == "title":
        section_3 = section_3.replace("Table 3-6:", "Table changed:")
    elif mutation == "metric":
        section_3 = section_3.replace("(MPH)", "(changed)")
    else:
        section_3 = section_3.replace(
            "All Aircraft 19,879,016 7,984,009 169 84% 24 64% 6 366",
            "All Aircraft 19,879,016 7,984,009 169 84%",
        )
    request = _faa_request(tmp_path, "section_3_capacity")
    with pytest.raises(AssortedSourcesError, match=message):
        parse_faa_table_text(
            section_3,
            table_id="3-6",
            rules={**source_rules, **source_rules["tables"]["3-6"]},
            request=request,
            checksum=request.expected_sha256,
        )


def test_faa_category_compatibility_failure(
    rules, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_rules = json.loads(json.dumps(rules["faa"]))
    section_3, section_4 = _faa_texts(source_rules)
    source_rules["tables"]["4-7"]["aircraft_category"] = "All Airplanes"
    section_4 = section_4.replace(
        "All Aircraft $2,760", "All Airplanes $2,760", 1
    )
    request_3 = _faa_request(tmp_path, "section_3_capacity")
    request_4 = _faa_request(tmp_path, "section_4_operating_costs")

    def fake_extract(request, *, document_title, checksum=None):
        assert document_title
        text = section_3 if request.component_id == "section_3_capacity" else section_4
        return text, checksum or request.expected_sha256

    monkeypatch.setattr(adapter, "extract_pdf_text", fake_extract)
    with pytest.raises(AssortedSourcesError, match="categories mismatch"):
        adapter.normalize_faa(request_3, request_4, source_rules)


def test_offline_fixture_smoke_writes_manifest_warnings_and_deterministic_outputs(
    bundle, rules, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_bundle = replace(bundle, repo_root=tmp_path)
    cache = tmp_path / "inputs" / "0_cache" / "fixtures"
    cache.mkdir(parents=True)
    nhtsa_path = cache / "nhtsa.zip"
    nems_path = cache / "nems.xlsx"
    gcam_path = cache / "gcam.csv"
    page_path = cache / "regen.html"
    payload_path = cache / "regen.js"
    faa_3_path = cache / "faa3.pdf"
    faa_4_path = cache / "faa4.pdf"
    _nhtsa_zip(nhtsa_path, rules["nhtsa_cafe"])
    _nems_workbook(nems_path, rules["eia_nems"])
    _gcam_csv(gcam_path, rules["jgcri_gcam"])
    page_path.write_text("<html>fixture</html>", encoding="utf-8")
    payload_path.write_text(_regen_payload(rules["epri_us_regen"]), encoding="utf-8")
    faa_3_path.write_bytes(b"%PDF-section-3-fixture")
    faa_4_path.write_bytes(b"%PDF-section-4-fixture")
    requests = {
        "nhtsa": _request(
            nhtsa_path,
            "zip",
            source_id=rules["nhtsa_cafe"]["source_id"],
            component_id=rules["nhtsa_cafe"]["component_id"],
        ),
        "nems": _request(
            nems_path,
            "xlsx",
            source_id=rules["eia_nems"]["source_id"],
            component_id=rules["eia_nems"]["component_id"],
        ),
        "gcam": _request(
            gcam_path,
            "csv",
            source_id=rules["jgcri_gcam"]["source_id"],
            component_id=rules["jgcri_gcam"]["component_id"],
        ),
        "regen_page": _request(
            page_path,
            "html",
            source_id=rules["epri_us_regen"]["source_id"],
            component_id=rules["epri_us_regen"]["component_id"],
        ),
        "faa_section_3": _request(
            faa_3_path,
            "pdf",
            source_id=rules["faa"]["source_id"],
            component_id=rules["faa"]["capacity_component_id"],
        ),
        "faa_section_4": _request(
            faa_4_path,
            "pdf",
            source_id=rules["faa"]["source_id"],
            component_id=rules["faa"]["costs_component_id"],
        ),
    }
    payload_request = _request(
        payload_path,
        "javascript",
        source_id=rules["epri_us_regen"]["source_id"],
        component_id=rules["epri_us_regen"]["component_id"],
    )
    section_3, section_4 = _faa_texts(rules["faa"])

    monkeypatch.setattr(adapter, "load_config_bundle", lambda _: local_bundle)
    monkeypatch.setattr(adapter, "module_rules", lambda _: rules)
    monkeypatch.setattr(adapter, "build_requests", lambda *_: requests)
    monkeypatch.setattr(
        adapter,
        "discover_regen_payload_request",
        lambda *_args, **_kwargs: (payload_request, "fixture-payload.js"),
    )

    def fake_extract(request, *, document_title, checksum=None):
        assert document_title
        text = section_3 if request.component_id == "section_3_capacity" else section_4
        return text, checksum or request.expected_sha256

    monkeypatch.setattr(adapter, "extract_pdf_text", fake_extract)

    class NoNetwork:
        def get(self, *_args, **_kwargs):
            raise AssertionError("offline smoke attempted network access")

    output_dir = adapter.fetch_and_normalize(
        "ignored.yaml", download=False, session=NoNetwork()
    )
    first_bytes = {
        path.name: path.read_bytes() for path in output_dir.iterdir() if path.is_file()
    }
    assert adapter.fetch_and_normalize(
        "ignored.yaml", download=False, session=NoNetwork()
    ) == output_dir
    assert first_bytes == {
        path.name: path.read_bytes() for path in output_dir.iterdir() if path.is_file()
    }
    manifest = pd.read_csv(output_dir / rules["manifest_file"])
    warnings = [
        json.loads(line)
        for line in (output_dir / rules["warnings_file"])
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert len(manifest) == 7
    assert set(manifest["cache_status"]) == {"cached"}
    assert set(manifest["status"]) == {"ok", "ok_with_warnings"}
    assert {
        "regen_currency_year_unresolved",
        "regen_configured_empty_series_excluded",
        "faa_currency_year_unresolved",
    } == {warning["code"] for warning in warnings}
    assert len(pd.read_csv(output_dir / rules["nhtsa_cafe"]["output_file"])) == 160
    assert len(pd.read_csv(output_dir / rules["eia_nems"]["output_file"])) == 102
    assert len(pd.read_csv(output_dir / rules["jgcri_gcam"]["output_file"])) == 120
    assert len(pd.read_csv(output_dir / rules["epri_us_regen"]["output_file"])) == 76
    assert len(pd.read_csv(output_dir / rules["faa"]["capacity_output_file"])) == 6
    assert len(pd.read_csv(output_dir / rules["faa"]["maintenance_output_file"])) == 2
