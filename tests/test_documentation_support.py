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
         "status": "reported", "population": "New registrations", "source": "statcan_new",
         "source_table": "20-10-0025-01"}
        for year in (2023, 2024) for group in ("Cars", "Light trucks")
        for fuel, value in (("gasoline", 9800), ("diesel", 0), ("hev", 0), ("bev", 199), ("phev", 1), ("other", 23))
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
    assert len(result.panel.unique()) == 6
    sizes = result.loc[result.dimension.eq("Size class") & result.vehicle_group.eq("Cars")]
    assert sizes.denominator.eq(601).all()
    assert sizes.loc[sizes.category.eq("Two-seater"), "icons"].iloc[0] == 0
    assert sizes.loc[sizes.category.eq("Two-seater"), "count"].iloc[0] == 1
    assert not result.dimension.eq("Mapping coverage").any()
    fuels = result.loc[result.dimension.eq("Powertrain")]
    assert fuels.source_year.eq(2024).all()
    assert fuels.carried_forward_years.eq(1).all()
    assert fuels.denominator.eq(10000).all()
    assert fuels.source.str.contains("20-10-0025-01 new registrations").all()
    assert not fuels.category.eq("other").any()
    assert "powertrain" not in sizes.columns  # independent marginal rows, no fabricated cross-tab


def test_top_models_are_canonical_counts_with_other_remainder(counts, settings):
    mto, stock = counts
    result = derive_fleet_shares(mto, stock, settings)
    models = result.loc[result.dimension.eq("Leading models") & result.vehicle_group.eq("Cars")]
    assert settings.top_models_per_vehicle_group == 5
    assert set(models.category) == {"Honda Civic", "Toyota Camry", "Toyota Corolla", "Mazda MX-5", "Other / unidentified mapped models"}
    assert models.loc[models.category.eq("Other / unidentified mapped models"), "count"].iloc[0] == 0
    assert models["count"].sum() == 601


def test_leading_models_keep_five_and_the_exact_remaining_count(counts, settings):
    mto, registrations = counts
    extra = pd.concat([
        mto.iloc[[0]].assign(FIT_ACTIVE=50, canonical_make="Acura", canonical_model="Integra", MAKE="ACUR", MODEL="INTEG"),
        mto.iloc[[0]].assign(FIT_ACTIVE=2, canonical_make="Tesla", canonical_model="Model 3", MAKE="TESL", MODEL="MODEL3"),
    ], ignore_index=True)
    result = derive_fleet_shares(pd.concat([mto, extra], ignore_index=True), registrations, settings)
    models = result.loc[result.dimension.eq("Leading models") & result.vehicle_group.eq("Cars")]
    assert len(models) == 6
    assert "Tesla Model 3" in set(models.category)
    assert "Mazda MX-5" not in set(models.category)
    assert models.loc[models.category.eq("Other / unidentified mapped models"), "count"].iloc[0] == 1
    assert models["count"].sum() == 653


def test_suppressed_represented_fuels_fail_without_fabricating_remainder(counts, settings):
    mto, stock = counts
    stock.loc[stock.year.eq(2024) & stock.powertrain.eq("bev"), "count"] = float("nan")
    with pytest.raises(ValueError, match="missing or suppressed"):
        derive_fleet_shares(mto, stock, settings)


def test_rejects_duplicate_mto_and_registered_stock_substitution(counts, settings):
    mto, stock = counts
    with pytest.raises(ValueError, match="duplicated"):
        derive_fleet_shares(pd.concat([mto, mto.iloc[:1]]), stock, settings)
    stock["source"] = "statcan_stock"
    with pytest.raises(ValueError, match="new registrations, not registered stock"):
        derive_fleet_shares(mto, stock, settings)


@pytest.fixture
def captured_fleet(tmp_path, monkeypatch, counts, settings):
    """A 2023 production handoff plus complete recorded history through 2025."""
    from dataclasses import replace
    from pathlib import Path
    import shutil
    from types import SimpleNamespace

    from parameterization.vehicle_reference import load_vehicle_reference, reference_mto_mapping
    from utils import harmonization_rule_paths, load_config_bundle, load_harmonization_rules
    import validation.documentation_support as support

    repository = Path(__file__).resolve().parents[1]
    parameters = tmp_path / "capture/config/parameters"
    parameters.mkdir(parents=True)
    bundle = load_config_bundle(repository / "config/scenarios/notebook_diagnostics.yaml")
    for path in (*harmonization_rule_paths(bundle), *(repository / "config/parameters" / name
                  for name in ("vehicle_reference.csv", "vehicle_source_links.csv"))):
        shutil.copyfile(path, parameters / path.name)
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={
        "config": bundle.paths.config.model_copy(update={"parameters": str(parameters)}),
    }))
    reference = load_vehicle_reference(bundle)
    mapping = reference_mto_mapping(
        reference, load_harmonization_rules(bundle, "road_aggregation"),
        load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["vehicle_class_harmonization"],
    )
    applicable = mapping.loc[mapping.model_year_from.le(2020) & mapping.model_year_to.ge(2020)]
    identities = [applicable.loc[applicable.nrcan_vehicle_class.eq(label)].iloc[0]
                  for label in ("Compact", "Mid-size", "Sport utility vehicle: Small")]
    records = []
    for year in (2023, 2025):
        for identity, count in zip(identities, (30, 10, 70), strict=True):
            records.append({
                "report_year": year, "VEHICLE_CLASS": "PASSENGER",
                "MAKE": identity.mto_make_code, "MODEL": identity.mto_model_code,
                "MODEL_YEAR": 2020, "stock_status": "FIT_ACTIVE",
                "native_count": count if year == 2025 else 999,
            })
    # A non-active stock status is never part of the presentation denominator.
    records.append({**records[-1], "stock_status": "UNFIT", "native_count": 123456})
    history = tmp_path / "capture/status_long.csv.gz"
    pd.DataFrame(records).to_csv(history, index=False)
    selected = tmp_path / "capture/mapped_fleet.csv"
    counts[0].assign(report_year=2023, FIT_ACTIVE=999).to_csv(selected, index=False)
    source_manifest = tmp_path / "capture/source_manifest.csv"
    pd.DataFrame({"year": [2023, 2025]}).to_csv(source_manifest, index=False)
    registrations = tmp_path / "capture/benchmark_statcan_new.csv"
    counts[1].to_csv(registrations, index=False)
    stock_manifest = tmp_path / "capture/stock_manifest.json"
    efficiency_manifest = tmp_path / "capture/efficiency_manifest.json"
    stock_manifest.write_text("{}", encoding="utf-8")
    efficiency_manifest.write_text("{}", encoding="utf-8")
    config = tmp_path / "documentation.yaml"
    config.write_text("fixture: true\n", encoding="utf-8")
    stock = SimpleNamespace(
        bundle=bundle, paths={"status_long": history, "mapped_fleet": selected, "source_manifest": source_manifest},
        metadata={"build_id": "stock-2023", "database": {"path": "stock.sqlite"}, "manifest": str(stock_manifest)},
    )
    efficiency = SimpleNamespace(
        bundle=bundle, paths={"benchmark_statcan_new": registrations},
        metadata={"build_id": "efficiency-2023", "database": {"path": "efficiency.sqlite"}, "manifest": str(efficiency_manifest)},
    )
    paths = SimpleNamespace(artifacts={"documentation_evidence": SimpleNamespace(path="evidence")})
    monkeypatch.setattr(support, "_controls", lambda _: (tmp_path, paths, settings, config))
    monkeypatch.setattr(support, "load_notebook_evidence",
                        lambda profile, **_: {"vehicle_stock": stock, "efficiency": efficiency}[profile])
    monkeypatch.setattr(support, "_statcan", lambda native, *_: native.copy())

    def forbid_live_configuration(*_, **__):
        raise AssertionError("Presentation must use the captured configuration bundle")

    monkeypatch.setattr(support, "load_config_bundle", forbid_live_configuration)
    read_csv = pd.read_csv

    def captured_read(path, *args, **kwargs):
        assert Path(path).resolve().is_relative_to(tmp_path), f"Uncaptured input: {path}"
        return read_csv(path, *args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", captured_read)
    return history, selected, support, tmp_path


def test_presentation_year_uses_captured_history_and_reviewed_mapping(captured_fleet):
    history, selected, support, root = captured_fleet
    evidence = support.build_fleet_100(repo_root=root)
    cars = evidence.shares.loc[evidence.shares.vehicle_group.eq("Cars") & evidence.shares.dimension.eq("Size class")]
    assert cars.source_year.eq(2025).all()
    assert cars.denominator.eq(40).all()
    assert cars.set_index("category")["count"].to_dict() == {"Compact": 30, "Mid-size": 10}
    assert cars.set_index("category").share.to_dict() == {"Compact": 0.75, "Mid-size": 0.25}
    trucks = evidence.shares.loc[evidence.shares.vehicle_group.eq("Light trucks") & evidence.shares.dimension.eq("Size class")]
    assert trucks.denominator.eq(70).all()
    assert evidence.metadata["mto_mapping"]["report_year"] == 2025
    assert evidence.metadata["stock_build"] == "stock-2023"
    assert {
        "mto_status_history", "mto_source_manifest", "reviewed_vehicle_reference",
        "reviewed_vehicle_source_links", "stock_rules", "benchmark_statcan_new",
    } <= set(evidence.metadata["input_files"])
    assert len([name for name in evidence.metadata["input_files"] if name.startswith("stock_rules:")]) == 6
    support.write_evidence(evidence, repo_root=root)
    selected.write_text("changed unused selected-year handoff", encoding="utf-8")
    pd.testing.assert_frame_equal(support.load_evidence(repo_root=root).shares, evidence.shares, check_dtype=False)
    history.write_bytes(b"changed recorded source")
    with pytest.raises(ValueError, match="Captured fleet presentation source changed"):
        support.load_evidence(repo_root=root)


@pytest.mark.parametrize("fault", ["missing_year", "duplicate", "missing_columns"])
def test_presentation_history_keeps_coverage_guards(captured_fleet, fault):
    history, _, support, root = captured_fleet
    statuses = pd.read_csv(history)
    if fault == "missing_year":
        statuses = statuses.loc[statuses.report_year.ne(2025)]
    elif fault == "duplicate":
        statuses = pd.concat([statuses, statuses.loc[statuses.report_year.eq(2025) & statuses.stock_status.eq("FIT_ACTIVE")].iloc[:1]])
    else:
        statuses = statuses.drop(columns="stock_status")
    statuses.to_csv(history, index=False)
    with pytest.raises(ValueError, match="coverage is missing or duplicated|status history lacks"):
        support.build_fleet_100(repo_root=root)


def test_archived_new_registration_lineage_is_retained(counts, settings):
    mto, registrations = counts
    registrations["source_table"] = "20-10-0021-01"
    result = derive_fleet_shares(mto, registrations, settings)
    assert result.loc[result.dimension.eq("Powertrain"), "source"].str.contains("20-10-0021-01").all()
    registrations["source_table"] = "23-10-0308-01"
    with pytest.raises(ValueError, match="source-table lineage"):
        derive_fleet_shares(mto, registrations, settings)


def test_infographic_is_static_complete_and_identified(counts, settings):
    result = derive_fleet_shares(*counts, settings)
    evidence = FleetPresentationEvidence(result, {}, settings)
    spec = fleet_100_chart(evidence).to_dict(validate=True)
    assert spec["usermeta"]["figure_id"] == "fleet-100-1"
    assert "separate views" in spec["title"]["text"]
    assert len(spec["vconcat"]) == 2
    assert all(len(row["hconcat"]) == 3 for row in spec["vconcat"][:2])
    assert "StatCan" in " ".join(spec["title"]["subtitle"])


def test_infographic_does_not_label_tiny_positive_shares_as_zero(counts, settings):
    import json
    mto, stock = counts
    stock.loc[stock.powertrain.eq("gasoline"), "count"] = 100000
    evidence = FleetPresentationEvidence(derive_fleet_shares(mto, stock, settings), {}, settings)
    spec = fleet_100_chart(evidence).to_dict(validate=True)
    rendered = json.dumps(spec)
    assert "Plug-in hybrid: <0.01% (n = 0)" in rendered
    assert "Plug-in hybrid: 0.00%" not in rendered
    assert "Other fuels:" not in rendered
    assert "n = # of symbols" in rendered
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
    rules_chunk = tmp_path / "vehicle_efficiency_and_emissions.yaml"
    rules_chunk.write_text("nonroad_classification: fixture\n", encoding="utf-8")
    report = {"ok": True, "database": str(database), "database_sha256": file_sha256(database),
              "timestamp_utc": "2026-01-01T00:00:00Z", "config": {"scenario": {"sha256": file_sha256(scenario_file)},
              "parameters": [{"path": str(path), "sha256": file_sha256(path)}
                             for path in (rules_file, rules_chunk)]}}
    report_file.write_text(json.dumps(report), encoding="utf-8")
    scenario = SimpleNamespace(outputs=SimpleNamespace(sqlite_name=database.name, validation_report=report_file.name),
                               scenario=SimpleNamespace(name="fixture"),
                               periods=SimpleNamespace(model=[2025, 2030], model_dump=lambda **_: {"model": [2025, 2030]}),
                               model_dump=lambda **_: {"scenario": "fixture"})
    paths = SimpleNamespace(artifacts={"documentation_evidence": SimpleNamespace(path="evidence")})
    monkeypatch.setattr(support, "_controls", lambda _: (tmp_path, paths, None, config_file))
    monkeypatch.setattr(support, "load_config_bundle", lambda _: SimpleNamespace(scenario=scenario))
    monkeypatch.setattr(support, "load_harmonization_rules", lambda *_: {"existing_suffix": "_EX"})
    def rules_fingerprints(_):
        return {path.name: file_sha256(path) for path in (rules_file, rules_chunk)}

    monkeypatch.setattr(support, "harmonization_rule_fingerprints", rules_fingerprints)
    monkeypatch.setattr(support, "resolve_configured_path", lambda *_: database)
    captured = support.build_nonroad_evidence(repo_root=tmp_path)
    assert not captured.rows.tech.str.endswith("_EX").any()
    assert len(captured.rows.loc[captured.rows.tech.eq("T_OFF")]) == 2
    original_chunk = rules_chunk.read_bytes()
    rules_chunk.write_bytes(original_chunk + b"# changed selected rules\n")
    with pytest.raises(ValueError, match="classification rules changed"):
        support.build_nonroad_evidence(repo_root=tmp_path)
    rules_chunk.write_bytes(original_chunk)
    calls = 0

    def changed_while_reading(bundle):
        nonlocal calls
        calls += 1
        values = rules_fingerprints(bundle)
        if calls > 1:
            values[rules_chunk.name] = "0" * 64
        return values

    with monkeypatch.context() as patch:
        patch.setattr(support, "harmonization_rule_fingerprints", changed_while_reading)
        with pytest.raises(ValueError, match="rules changed while"):
            support.build_nonroad_evidence(repo_root=tmp_path)
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
