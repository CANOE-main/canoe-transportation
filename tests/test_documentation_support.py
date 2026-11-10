"""Presentation rounding never invents joint distributions or conceals coverage."""

import pandas as pd
import pytest

from utils import load_yaml
from validation.documentation_support import (
    FleetPresentation, FleetPresentationEvidence, derive_fleet_shares,
    fleet_100_chart, largest_remainder, NonroadPresentation,
    NonroadPresentationEvidence, nonroad_efficiency_chart, validate_nonroad_rows,
)


@pytest.fixture
def settings():
    from pathlib import Path
    return FleetPresentation.model_validate(load_yaml(Path("config/parameters/documentation.yaml"))["fleet_100"])


@pytest.fixture
def counts():
    mto = pd.DataFrame([
        {"report_year": 2025, "FIT_ACTIVE": count, "mapping_accepted": accepted,
         "mapping_outcome": "mapped_ldv" if accepted else "unmapped", "nrcan_ceud_class": broad,
         "nrcan_vehicle_class": size, "canonical_make": make, "canonical_model": model,
         "VEHICLE_CLASS": "PASSENGER", "MAKE": make, "MODEL": model, "MODEL_YEAR": 2020}
        for count, accepted, broad, size, make, model in [
            (300, True, "Car", "Compact", "Honda", "Civic"),
            (200, True, "Car", "Mid-size", "Toyota", "Camry"),
            (100, True, "Car", "Compact", "Toyota", "Corolla"),
            (1, True, "Car", "Two-seater", "Mazda", "MX-5"),
            (400, True, "Light Truck", "Sport utility vehicle: Small", "Toyota", "RAV4"),
            (300, True, "Light Truck", "Pickup truck: Standard", "Ford", "F-150"),
            (200, True, "Light Truck", "Minivan", "Honda", "Odyssey"),
            (3000, False, None, None, "Unresolved", "Unresolved"),
        ]
    ])
    statcan = pd.DataFrame([
        {"region": "ON", "year": year, "vehicle_group": group, "powertrain": fuel, "count": value,
         "status": "reported", "population": "Registered stock"}
        for year in (2023, 2024) for group in ("Cars", "Light trucks")
        for fuel, value in (("all", 10000), ("gasoline", 9800), ("bev", 199), ("phev", 1), ("other", 0))
    ])
    return mto, statcan


def test_largest_remainder_preserves_tiny_positive_and_exact_total():
    counts = pd.Series({"A": 4999, "B": 5000, "tiny": 1, "zero": 0})
    result = largest_remainder(counts, 100)
    assert result.sum() == 100
    assert result["tiny"] == 0
    assert result["zero"] == 0
    pd.testing.assert_series_equal(counts, pd.Series({"A": 4999, "B": 5000, "tiny": 1, "zero": 0}))
    assert largest_remainder(pd.Series({"B": 1, "A": 1, "C": 1}), 100)["A"] == 34


def test_marginals_keep_denominators_and_unmapped_population_separate(counts, settings):
    mto, stock = counts
    result = derive_fleet_shares(mto, stock, settings)
    assert result.groupby("panel").icons.sum().eq(100).all()
    assert len(result.panel.unique()) == 7
    sizes = result.loc[result.dimension.eq("Size class") & result.vehicle_group.eq("Cars")]
    assert sizes.denominator.eq(601).all()
    assert sizes.loc[sizes.category.eq("Two-seater"), "icons"].iloc[0] == 0
    assert sizes.loc[sizes.category.eq("Two-seater"), "count"].iloc[0] == 1
    coverage = result.loc[result.dimension.eq("Mapping coverage")]
    assert coverage.denominator.eq(4501).all()
    assert coverage.loc[coverage.category.eq("Unresolved classification"), "count"].iloc[0] == 3000
    fuels = result.loc[result.dimension.eq("Powertrain")]
    assert fuels.source_year.eq(2024).all()
    assert fuels.carried_forward_years.eq(1).all()
    assert fuels.denominator.eq(10000).all()
    assert fuels.source.str.contains("registered stock").all()
    assert "powertrain" not in sizes.columns  # independent marginal rows, no fabricated cross-tab


def test_top_models_are_canonical_counts_with_other_remainder(counts, settings):
    mto, stock = counts
    result = derive_fleet_shares(mto, stock, settings)
    models = result.loc[result.dimension.eq("Leading models") & result.vehicle_group.eq("Cars")]
    assert set(models.category) == {"Honda Civic", "Toyota Camry", "Toyota Corolla", "Other / unidentified mapped models"}
    assert models.loc[models.category.eq("Other / unidentified mapped models"), "count"].iloc[0] == 1
    assert models["count"].sum() == 601


def test_latest_suppressed_stock_cells_remain_remainder_not_zero(counts, settings):
    mto, stock = counts
    stock.loc[stock.year.eq(2024) & stock.powertrain.eq("bev"), "count"] = float("nan")
    result = derive_fleet_shares(mto, stock, settings)
    suppressed = result.loc[result.category.eq("Unreported / suppressed fuel remainder")]
    assert len(suppressed) == 2
    assert suppressed["count"].eq(199).all()
    assert suppressed.source_year.eq(2024).all()


def test_rejects_duplicate_mto_and_invalid_statcan_total(counts, settings):
    mto, stock = counts
    with pytest.raises(ValueError, match="duplicated"):
        derive_fleet_shares(pd.concat([mto, mto.iloc[:1]]), stock, settings)
    stock.loc[stock.powertrain.eq("all"), "count"] = 10
    with pytest.raises(ValueError, match="exceed"):
        derive_fleet_shares(mto, stock, settings)


def test_infographic_is_static_complete_and_identified(counts, settings):
    result = derive_fleet_shares(*counts, settings)
    evidence = FleetPresentationEvidence(result, {}, settings)
    spec = fleet_100_chart(evidence).to_dict(validate=True)
    assert spec["usermeta"]["figure_id"] == "fleet-100-1"
    assert "separate views" in spec["title"]["text"]
    assert len(spec["vconcat"]) == 3
    assert all(len(row["hconcat"]) == 3 for row in spec["vconcat"][:2])
    assert "StatCan" in " ".join(spec["title"]["subtitle"])


def test_infographic_does_not_label_tiny_positive_shares_as_zero(counts, settings):
    import json
    mto, stock = counts
    stock.loc[stock.powertrain.eq("all"), "count"] = 10001
    stock.loc[stock.powertrain.eq("other"), "count"] = 1
    evidence = FleetPresentationEvidence(derive_fleet_shares(mto, stock, settings), {}, settings)
    spec = fleet_100_chart(evidence).to_dict(validate=True)
    rendered = json.dumps(spec)
    assert "Other fuels: <0.01% (0 symbols)" in rendered
    assert "Other fuels: 0.00%" not in rendered
    assert "#222222" in rendered
    assert "rounded to 0.01%" in " ".join(spec["title"]["subtitle"])


@pytest.fixture
def nonroad_settings():
    from pathlib import Path
    return NonroadPresentation.model_validate(load_yaml(Path("config/parameters/documentation.yaml"))["nonroad"])


def _nonroad_rows(settings):
    return pd.DataFrame([
        {"region": "ON", "tech": "T_OFF" if mode == "off_road" else f"{mode}_diesel", "category": mode, "sub_category": "diesel", "vintage": vintage,
         "input_comm": "T_dsl", "output_comm": f"service_{mode}", "efficiency": 1.0,
         "units": "PJ/PJ" if mode == "off_road" else "bn passenger-km/PJ" if mode.startswith("passenger") else "bn tonne-km/PJ"}
        for mode in settings.modes for vintage in (2025, 2030)
    ])


def test_nonroad_panels_keep_mode_units_and_static_build_identity(nonroad_settings):
    rows = _nonroad_rows(nonroad_settings)
    evidence = NonroadPresentationEvidence(rows, {"scenario": "fixture", "build_timestamp_utc": "2026-01-01T00:00:00Z"}, nonroad_settings)
    spec = nonroad_efficiency_chart(evidence).to_dict(validate=True)
    assert spec["usermeta"]["figure_id"] == "nonroad-1"
    assert len(spec["vconcat"]) == 3
    units = [panel["encoding"]["y"]["title"] for row in spec["vconcat"] for panel in row["hconcat"]]
    assert units.count("bn passenger-km/PJ") == 2
    assert units.count("bn tonne-km/PJ") == 3
    assert units.count("PJ/PJ") == 1
    assert "2026-01-01" in " ".join(spec["title"]["subtitle"])
    rows.loc[0, "units"] = "PJ/PJ"
    with pytest.raises(ValueError, match="one explicit unit"):
        validate_nonroad_rows(rows, nonroad_settings)


def test_nonroad_capture_checks_database_identity_and_snapshot_is_read_only(tmp_path, monkeypatch, nonroad_settings):
    import json
    import sqlite3
    from types import SimpleNamespace
    import validation.documentation_support as support
    from utils import file_sha256

    rows = _nonroad_rows(nonroad_settings)
    # Legacy timing may put existing and new vehicles at the same first model vintage.
    existing = rows.iloc[[0]].assign(tech="old_air_EX", efficiency=999.0)
    rows_with_existing = pd.concat([rows, existing], ignore_index=True)
    database = tmp_path / "baseline.sqlite"
    with sqlite3.connect(database) as connection:
        rows_with_existing.drop(columns=["category", "sub_category"]).to_sql("efficiency", connection, index=False)
        rows_with_existing[["tech", "category", "sub_category"]].drop_duplicates().to_sql("technology", connection, index=False)
        pd.DataFrame({"region": ["ON"], "tech": [rows.tech.iloc[0]], "units": ["bn passenger-km"]}).to_sql("existing_capacity", connection, index=False)
    scenario_file = tmp_path / "scenario.yaml"
    scenario_file.write_text("fixture: true\n", encoding="utf-8")
    settings = nonroad_settings.model_copy(update={"scenario": "scenario.yaml"})
    config_file = tmp_path / "documentation.yaml"
    config_file.write_text(json.dumps({"nonroad": settings.model_dump()}), encoding="utf-8")
    report_file = tmp_path / "report.json"
    rules_file = tmp_path / "rules.yaml"
    rules_file.write_text("existing_suffix: _EX\n", encoding="utf-8")
    report = {"ok": True, "database": str(database), "database_sha256": file_sha256(database),
              "timestamp_utc": "2026-01-01T00:00:00Z", "config": {"scenario": {"sha256": file_sha256(scenario_file)},
              "parameters": [{"path": str(rules_file), "sha256": file_sha256(rules_file)}]}}
    report_file.write_text(json.dumps(report), encoding="utf-8")
    scenario = SimpleNamespace(outputs=SimpleNamespace(sqlite_name=database.name, validation_report=report_file.name),
                               scenario=SimpleNamespace(name="fixture"),
                               periods=SimpleNamespace(model=[2025, 2030], model_dump=lambda **_: {"model": [2025, 2030]}),
                               model_dump=lambda **_: {"scenario": "fixture"})
    paths = SimpleNamespace(artifacts={"documentation_evidence": SimpleNamespace(path="evidence")})
    monkeypatch.setattr(support, "_controls", lambda _: (tmp_path, paths, None, config_file))
    monkeypatch.setattr(support, "load_config_bundle", lambda _: SimpleNamespace(scenario=scenario))
    monkeypatch.setattr(support, "load_harmonization_rules", lambda *_: {"existing_suffix": "_EX"})
    monkeypatch.setattr(support, "resolve_configured_path", lambda *_: database)
    captured = support.build_nonroad_evidence(repo_root=tmp_path)
    assert not captured.rows.tech.str.endswith("_EX").any()
    assert len(captured.rows.loc[captured.rows.tech.eq("T_OFF")]) == 2
    support.write_nonroad_evidence(captured, repo_root=tmp_path)
    database.write_bytes(b"changed after capture")
    loaded = support.load_nonroad_evidence(repo_root=tmp_path)
    assert len(loaded.rows) == len(rows)  # independent of the later live database
    with pytest.raises(ValueError, match="actual database bytes"):
        support.build_nonroad_evidence(repo_root=tmp_path)
    table = tmp_path / "evidence" / settings.exact_table
    table.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="rows changed"):
        support.load_nonroad_evidence(repo_root=tmp_path)
