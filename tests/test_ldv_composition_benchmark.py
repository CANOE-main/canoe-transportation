"""Independent count populations, time bases and conditional denominators."""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from validation import ldv_composition_benchmark as benchmark


def reference_for(entries):
    """Small reviewed source-link fixture, independent of live configuration."""
    from utils.vehicle_labels import normalize_vehicle_text

    canonical, links = [], []
    for make, model, canonical_make, canonical_model, year, powertrain in entries:
        identifier = normalize_vehicle_text(canonical_make) + "/" + normalize_vehicle_text(canonical_model)
        canonical.append({"vehicle_id": identifier, "canonical_make": canonical_make,
                          "canonical_model": canonical_model, "model_year_from": year,
                          "model_year_to": year, "evidence_status": "catalogue_supported"})
        links.append({"source": "transport_canada_izev", "source_make": make, "source_model": model,
                      "model_year_from": year, "model_year_to": year, "observed_powertrain": powertrain,
                      "source_vehicle_class": "", "vehicle_id": identifier, "vehicle_scope": "ldv",
                      "link_status": "matched_rating_family", "canonical_description": canonical_model,
                      "source_notes": ""})
    return SimpleNamespace(canonical=pd.DataFrame(canonical).drop_duplicates(), links=pd.DataFrame(links))


@pytest.fixture
def setup(monkeypatch):
    fuel_map = {"All fuel types": "all", "Gasoline": "gasoline", "Diesel": "diesel",
                "Battery electric": "bev", "Hybrid electric": "hev", "Plug-in hybrid electric": "phev", "Other fuel types": "other"}
    body_map = {"Passenger cars": "Cars", "Pickup trucks": "Light trucks", "Multi-purpose vehicles": "Light trucks", "Vans": "Light trucks"}
    spec = {"fuel_map": fuel_map, "body_map": body_map, "incentive_transaction_types": ["Purchase", "Lease"], "comparison_calendar_years": [2020, 2021, 2022, 2023, 2024],
            "new_registrations_file": "new.csv", "stock_file": "stock.csv", "mto_passenger_file": "passenger.csv",
            "mto_commercial_file": "commercial.csv", "mto_mapped_file": "mapped.csv", "izev_file": "izev.csv"}
    class_map = {"unmapped_policy": "error", "target_to_nrcan": {"Compact": ["Compact"], "Midsize": ["Mid-size"], "SUV": ["Sport utility vehicle: Small", "Sport utility vehicle: Standard"]},
                 "target_to_ceud": {"Compact": "Car", "Midsize": "Car", "SUV": "Light Truck"}}
    config = {
        "ldv_composition_benchmark": spec,
        "statcan_tables": {"interim_subdir": "statcan", "geography": {"ON": {"label": "Ontario"}, "MB": {"label": "Manitoba"}}},
        "nrcan_fuel_consumption_ratings": {"vehicle_class_harmonization": class_map},
        "efficiencies": {"historical_suv": {"source_label": "Sport utility vehicle", "combined_weight_classes": ["Sport utility vehicle: Small", "Sport utility vehicle: Standard"]},
                         "ratings": {"composition": {"aliases": [], "izev_aliases": [], "province_map": {}}}},
    }
    monkeypatch.setattr(benchmark, "load_harmonization_rules", lambda _bundle, key: config[key])
    bundle = SimpleNamespace(scenario=SimpleNamespace(geography=SimpleNamespace(regions=["ON"])))
    rows = []
    bev = {"Passenger cars": 20, "Pickup trucks": 5, "Multi-purpose vehicles": 10, "Vans": 5}
    phev = {"Passenger cars": 10, "Pickup trucks": 10, "Multi-purpose vehicles": 20, "Vans": 0}
    stock_totals = {"Passenger cars": 1000, "Pickup trucks": 400, "Multi-purpose vehicles": 500, "Vans": 100}
    for body in body_map:
        for native, powertrain in fuel_map.items():
            for source in ["new", "stock"]:
                if source == "new" and powertrain == "all":
                    continue
                count = bev[body] if powertrain == "bev" else phev[body] if powertrain == "phev" else 100
                if source == "stock" and powertrain == "all":
                    count = stock_totals[body]
                rows.append({"source": source, "scenario_region": "ON", "geography": "Ontario",
                             "reference_year": 2024, "reference_period": 2024, "vehicle_type": body if source == "new" else body + ", weighing 4,536 kilograms or less",
                             "fuel_type": native, "scaled_value": count, "table_id": "20-10-0025-01" if source == "new" else "23-10-0308-01"})
    statcan = pd.DataFrame(rows)
    records = []
    for make, model, powertrain, model_year, purchase, count in [
        ("Tesla", "Model 3", "bev", 2023, "Purchase", 3), ("Tesla", "Model 3", "bev", 2023, "Lease", 1),
        ("Toyota", "Prius", "phev", 2022, "Purchase", 1), ("Toyota", "Prius", "phev", 2022, "Lease", 1),
        ("Tesla", "Model Y", "bev", 2023, "Purchase", 2), ("Tesla", "Model Y", "bev", 2023, "Lease", 2),
        ("Toyota", "RAV4", "phev", 2023, "Purchase", 2), ("Nissan", "Unmatched", "bev", 2023, "Purchase", 1),
        ("Toyota", "Mirai", "fcev", 2023, "Purchase", 1),
    ]:
        records.append({"make": make, "model": model, "powertrain": powertrain, "model_year": model_year,
                        "request_year": 2024, "province": "ON", "Purchase or Lease": purchase, "count": count})
    observations = []
    for index, (make, model, powertrain, year, rating_class) in enumerate([
        ("Tesla", "Model 3", "bev", 2023, "Compact"), ("Toyota", "Prius", "phev", 2022, "Compact"),
        ("Tesla", "Model Y", "bev", 2023, "Sport utility vehicle: Small"), ("Toyota", "RAV4", "phev", 2023, "Sport utility vehicle: Small"),
    ]):
        observations.append({"source_kind": "izev", "year": year, "family_make": make, "family_model": model,
                             "vehicle_id": make.upper() + "/" + model.replace(" ", "").upper(),
                             "observed_powertrain": powertrain, "rating_class": rating_class, "candidate_rating_classes": rating_class,
                             "candidate_powertrains": powertrain, "resolved_powertrain": powertrain,
                             "empirical_observation_id": f"i{index}", "match_status": "outside_supported_program_window", "source_count": 1, "region": "ON"})
    for index, (powertrain, rating_class, count) in enumerate([("gasoline", "Compact", 50), ("hev", "Sport utility vehicle: Small", 100)]):
        observations.append({"source_kind": "mto", "year": 2023, "family_make": "Fixture", "family_model": str(index),
                             "observed_powertrain": "", "rating_class": rating_class, "candidate_rating_classes": rating_class,
                             "candidate_powertrains": powertrain, "resolved_powertrain": powertrain, "empirical_observation_id": f"m{index}",
                             "match_status": "resolved", "source_count": count, "region": "ON"})
    # Repeated Ontario evidence in another requested-region audit is not another fleet.
    observations.append({**observations[-2], "region": "MB"})
    cohort_columns = ["population_group", "report_year", "model_year", "cohort_class", "stock_status", "cohort_count"]
    passenger = pd.DataFrame([
        ["mapped_passenger", 2024, 2000, "Compact", "FIT_ACTIVE", 100],
        ["mapped_passenger", 2024, 2000, "Sport utility vehicle: Small", "FIT_ACTIVE", 300],
        ["mapped_passenger", 2024, 2000, "Compact", "NON_FIT_ACTIVE_PROXY", 900],
        ["mapped_passenger", 2025, 2000, "Compact", "FIT_ACTIVE", 200],
    ], columns=cohort_columns)
    commercial = pd.DataFrame([["mapped_commercial", 2024, 2010, "Sport utility vehicle: Small", "FIT_ACTIVE", 100]], columns=cohort_columns)
    mapped = pd.DataFrame([
        {"report_year": 2025, "mapping_accepted": True, "nrcan_ceud_class": "Car", "nrcan_vehicle_class": "Compact", "FIT_ACTIVE": 1000},
        {"report_year": 2025, "mapping_accepted": True, "nrcan_ceud_class": "Light Truck", "nrcan_vehicle_class": "Sport utility vehicle: Small", "FIT_ACTIVE": 2000},
    ])
    inputs = {"benchmark_statcan_new": statcan.loc[statcan.source.eq("new")].drop(columns="source"),
              "benchmark_statcan_stock": statcan.loc[statcan.source.eq("stock")].drop(columns="source"),
              "benchmark_mto_passenger_cohorts": passenger, "benchmark_mto_commercial_cohorts": commercial,
              "benchmark_mto_mapped": mapped, "benchmark_izev_records": pd.DataFrame(records),
              "composition_observations": pd.DataFrame(observations)}
    reference = reference_for([
        (row["family_make"], row["family_model"], row["family_make"], row["family_model"],
         row["year"], row["observed_powertrain"])
        for row in observations if row["source_kind"] == "izev"
    ])
    monkeypatch.setattr(benchmark, "load_vehicle_reference", lambda captured: reference)
    return bundle, inputs, spec


def test_acquisition_comparison_includes_leases_and_uses_request_calendar_year(setup):
    bundle, inputs, _ = setup
    result = benchmark.build_composition_benchmark(bundle, inputs)
    rows = result.comparison
    cars = rows.loc[rows.comparison_scope.eq("new_pev_powertrain_share") & rows.vehicle_group.eq("Cars")]
    assert set(cars.year) == {2024}
    assert cars.loc[cars.source.eq("izev")].set_index("powertrain")["count"].to_dict() == {"bev": 4, "phev": 2}
    assert cars.loc[cars.source.eq("izev")].set_index("powertrain").share.to_dict() == {"bev": 2 / 3, "phev": 1 / 3}
    assert cars.loc[cars.source.eq("statcan_new")].set_index("powertrain").share.to_dict() == {"bev": 2 / 3, "phev": 1 / 3}
    assert result.timing.year_lag.max() == 2
    assert result.timing["count"].sum() == 13  # FCEV excluded from PEV comparison, unknown class retained in timing.
    assert set(result.timing.model_year) == {2022, 2023}
    assert "Lease" in set(result.observations.loc[result.observations.source.eq("izev"), "transaction_types"])


def test_unmatched_classes_remain_coverage_gaps_and_do_not_enter_shares(setup):
    bundle, inputs, _ = setup
    result = benchmark.build_composition_benchmark(bundle, inputs)
    coverage = result.coverage.loc[result.coverage.source.eq("izev")].set_index("powertrain")
    assert coverage.loc["bev", "total_count"] == 9
    assert coverage.loc["bev", "mapped_count"] == 8
    assert coverage.loc["bev", "coverage_share"] == 8 / 9
    assert coverage.loc["bev", "status"] == "class_mapping_partial"
    assert set(result.comparison.loc[result.comparison.source.eq("izev"), "powertrain"]) == {"bev", "phev"}
    assert result.comparison.loc[result.comparison.source.eq("izev"), "denominator_basis"].str.contains("BEV plus PHEV|Cars plus light trucks").all()
    raw = result.observations.loc[result.observations.source.eq("izev")]
    assert raw["count"].sum() == 13
    unknown = raw.loc[raw.source_model.eq("Unmatched")]
    assert unknown.source_make.item() == "Nissan"
    assert unknown.status.item() == "unmatched_or_ambiguous_class"
    assert unknown.vehicle_group.item() == ""
    assert not unknown.in_acquisition_comparison.item()


def test_range_ambiguity_does_not_hide_an_unambiguous_broad_vehicle_class(setup):
    bundle, inputs, _ = setup
    rows = inputs["composition_observations"]
    index = rows.index[rows.source_kind.eq("izev") & rows.family_model.eq("Model Y")]
    rows.loc[index, "rating_class"] = ""
    rows.loc[index, "candidate_powertrains"] = "bev_200_miles | bev_300_miles"
    rows.loc[index, "resolved_powertrain"] = ""
    rows.loc[index, "match_status"] = "ambiguous_powertrain_or_range"
    result = benchmark.build_composition_benchmark(bundle, inputs)
    local = result.observations.loc[result.observations.source.eq("izev") & result.observations.vehicle_group.eq("Light trucks") & result.observations.powertrain.eq("bev")]
    assert local["count"].sum() == 4
    assert result.coverage.loc[result.coverage.source.eq("izev") & result.coverage.powertrain.eq("bev"), "mapped_count"].item() == 8


def test_different_fine_classes_are_accepted_only_at_their_unanimous_broad_grain(setup):
    bundle, inputs, _ = setup
    rows = inputs["composition_observations"]
    index = rows.index[rows.source_kind.eq("izev") & rows.family_model.eq("Model 3")]
    rows.loc[index, "rating_class"] = ""
    rows.loc[index, "candidate_rating_classes"] = "Compact | Mid-size"
    rows.loc[index, "match_status"] = "ambiguous_rating_class"
    result = benchmark.build_composition_benchmark(bundle, inputs)
    native = result.observations.loc[result.observations.source_model.eq("Model 3")]
    assert native.vehicle_group.eq("Cars").all()
    assert native.mapped_rating_classes.eq("Compact | Mid-size").all()
    assert native["count"].sum() == 4
    assert result.coverage.loc[result.coverage.source.eq("izev") & result.coverage.powertrain.eq("bev"), "mapped_count"].item() == 8


@pytest.mark.parametrize("classes", ["Compact | Sport utility vehicle: Small", "Compact | Unknown class"])
def test_conflicting_or_unknown_candidate_classes_cannot_be_hidden_by_a_resolved_label(setup, classes):
    bundle, inputs, _ = setup
    rows = inputs["composition_observations"]
    index = rows.index[rows.source_kind.eq("izev") & rows.family_model.eq("Model 3")]
    rows.loc[index, "candidate_rating_classes"] = classes
    result = benchmark.build_composition_benchmark(bundle, inputs)
    native = result.observations.loc[result.observations.source_model.eq("Model 3")]
    assert native.vehicle_group.eq("").all()
    assert native.status.eq("unmatched_or_ambiguous_class").all()
    assert not native.in_acquisition_comparison.any()
    assert result.coverage.loc[result.coverage.source.eq("izev") & result.coverage.powertrain.eq("bev"), "mapped_count"].item() == 4


def test_partial_program_calendar_years_are_excluded_only_from_comparison_panels(setup):
    bundle, inputs, _ = setup
    extra = inputs["benchmark_izev_records"].iloc[[0]].copy()
    extra["request_year"] = 2025
    inputs["benchmark_izev_records"] = pd.concat([inputs["benchmark_izev_records"], extra], ignore_index=True)
    result = benchmark.build_composition_benchmark(bundle, inputs)
    acquisition = result.comparison.loc[result.comparison.comparison_scope.str.startswith("new_pev")]
    assert set(acquisition.year) == {2024}
    assert 2025 in set(result.timing.request_year)
    assert result.timing.loc[result.timing.request_year.eq(2025), "in_comparison_calendar_years"].eq(False).all()
    assert result.coverage.loc[result.coverage.year.eq(2025) & result.coverage.source.eq("izev"), "in_acquisition_comparison"].eq(False).all()


def test_stock_class_shares_use_same_report_year_and_published_all_fuel_totals(setup):
    bundle, inputs, _ = setup
    result = benchmark.build_composition_benchmark(bundle, inputs)
    stock = result.comparison.loc[result.comparison.comparison_scope.eq("stock_vehicle_group_share")]
    assert set(stock.year) == {2024}
    mto = stock.loc[stock.source.eq("mto_stock")].set_index("vehicle_group")
    assert mto["count"].to_dict() == {"Cars": 100, "Light trucks": 400}
    assert mto.share.to_dict() == {"Cars": .2, "Light trucks": .8}
    statcan = stock.loc[stock.source.eq("statcan_stock")].set_index("vehicle_group")
    assert statcan["count"].to_dict() == {"Cars": 1000, "Light trucks": 1000}
    assert statcan.share.to_dict() == {"Cars": .5, "Light trucks": .5}


def test_repeated_passenger_partition_cannot_be_added_as_commercial_stock(setup):
    bundle, inputs, _ = setup
    inputs["benchmark_mto_commercial_cohorts"] = inputs["benchmark_mto_passenger_cohorts"].copy()
    with pytest.raises(ValueError, match="disjoint Report A"):
        benchmark.build_composition_benchmark(bundle, inputs)


def test_mto_powertrain_is_explicitly_subset_conditioned_not_full_fleet_distribution(setup):
    bundle, inputs, _ = setup
    result = benchmark.build_composition_benchmark(bundle, inputs)
    rows = result.comparison.loc[result.comparison.source.eq("mto_catalogue_subset")]
    assert set(rows.comparison_scope) == {"mto_catalogue_subset_powertrain"}
    assert set(rows.year) == {2025}
    assert rows["count"].sum() == 150  # Repeated region audit excluded.
    assert rows.native_powertrain.eq("Unobserved").all()
    assert rows.comparability_note.str.contains("Coverage-only").all()
    coverage = result.coverage.loc[result.coverage.source.eq("mto_catalogue_subset")]
    assert coverage.coverage_share.eq(.05).all()


def test_historical_unsplit_suv_uses_the_existing_broad_class_harmonization(setup):
    bundle, inputs, _ = setup
    mask = inputs["composition_observations"].source_kind.eq("mto") & inputs["composition_observations"].resolved_powertrain.eq("hev")
    inputs["composition_observations"].loc[mask, ["rating_class", "candidate_rating_classes"]] = "Sport utility vehicle"
    result = benchmark.build_composition_benchmark(bundle, inputs)
    rows = result.comparison.loc[result.comparison.source.eq("mto_catalogue_subset")]
    assert rows.loc[rows.powertrain.eq("hev"), "vehicle_group"].item() == "Light trucks"
    assert rows.loc[rows.powertrain.eq("hev"), "count"].item() == 100


def test_historical_mto_nlr_classes_are_not_mistaken_for_nrcan_source_classes(setup):
    bundle, inputs, _ = setup
    for name in ["benchmark_mto_passenger_cohorts", "benchmark_mto_commercial_cohorts"]:
        rows = inputs[name]
        rows.loc[rows.cohort_class.eq("Sport utility vehicle: Small"), "cohort_class"] = "SUV"
    result = benchmark.build_composition_benchmark(bundle, inputs)
    stock = result.comparison.loc[result.comparison.comparison_scope.eq("stock_vehicle_group_share") & result.comparison.source.eq("mto_stock")]
    assert stock.set_index("vehicle_group")["count"].to_dict() == {"Cars": 100, "Light trucks": 400}
    assert stock.set_index("vehicle_group").share.to_dict() == {"Cars": .2, "Light trucks": .8}


def test_missing_truck_body_fuel_cell_does_not_silently_renormalize_statcan(setup):
    bundle, inputs, _ = setup
    frame = inputs["benchmark_statcan_new"]
    inputs["benchmark_statcan_new"] = frame.loc[~(frame.vehicle_type.eq("Vans") & frame.fuel_type.eq("Plug-in hybrid electric"))]
    result = benchmark.build_composition_benchmark(bundle, inputs)
    cells = result.comparison.loc[result.comparison.comparison_scope.eq("new_pev_powertrain_share") & result.comparison.source.eq("statcan_new") & result.comparison.vehicle_group.eq("Light trucks")]
    assert cells.share.isna().all()
    assert cells.loc[cells.powertrain.eq("phev"), "status"].item() == "missing_or_suppressed_body_fuel_cells"


def test_absent_entire_fuel_series_is_not_treated_as_zero(setup):
    bundle, inputs, _ = setup
    frame = inputs["benchmark_statcan_new"]
    inputs["benchmark_statcan_new"] = frame.loc[~frame.fuel_type.eq("Plug-in hybrid electric")]
    result = benchmark.build_composition_benchmark(bundle, inputs)
    cells = result.comparison.loc[result.comparison.comparison_scope.eq("new_pev_powertrain_share") & result.comparison.source.eq("statcan_new")]
    assert cells.share.isna().all()


def test_missing_entire_reference_year_is_explicit_without_zero_counts_or_pairing(setup):
    bundle, inputs, _ = setup
    inputs["benchmark_statcan_new"] = inputs["benchmark_statcan_new"].iloc[:0]
    result = benchmark.build_composition_benchmark(bundle, inputs)
    gap = result.coverage.loc[result.coverage.source.eq("statcan_new") & result.coverage.year.eq(2024)]
    assert gap.status.item() == "missing_reference_region_year"
    assert gap.total_count.isna().all() and gap.coverage_share.isna().all()
    rows = result.comparison.loc[result.comparison.comparison_scope.str.startswith("new_pev")]
    assert rows.source.eq("izev").all()
    assert rows.status.eq("unpaired_missing_statcan_reference").all()
    assert not rows.counterpart_source_available.any()
    assert rows["count"].gt(0).all()


def test_missing_reference_region_does_not_borrow_another_regions_counts(setup):
    bundle, inputs, _ = setup
    bundle.scenario.geography.regions = ["ON", "MB"]
    extra = inputs["benchmark_izev_records"].iloc[[0]].copy()
    extra["province"] = "MB"
    inputs["benchmark_izev_records"] = pd.concat([inputs["benchmark_izev_records"], extra], ignore_index=True)
    result = benchmark.build_composition_benchmark(bundle, inputs)
    gap = result.coverage.loc[result.coverage.region.eq("MB") & result.coverage.year.eq(2024) & result.coverage.source.eq("statcan_new")]
    assert gap.status.item() == "missing_reference_region_year"
    assert gap.total_count.isna().all()
    rows = result.comparison.loc[result.comparison.region.eq("MB") & result.comparison.comparison_scope.str.startswith("new_pev")]
    assert rows.source.eq("izev").all()
    assert rows.status.eq("unpaired_missing_statcan_reference").all()
    assert not rows.counterpart_source_available.any()


def test_missing_incentive_year_is_a_gap_rather_than_zero_market_share(setup):
    bundle, inputs, _ = setup
    inputs["benchmark_izev_records"]["request_year"] = 2025
    result = benchmark.build_composition_benchmark(bundle, inputs)
    gap = result.coverage.loc[result.coverage.source.eq("izev") & result.coverage.year.eq(2024)]
    assert gap.status.item() == "no_recorded_incentive_region_year"
    assert gap.total_count.isna().all() and gap.coverage_share.isna().all()
    rows = result.comparison.loc[result.comparison.comparison_scope.str.startswith("new_pev")]
    assert rows.source.eq("statcan_new").all()
    assert rows.status.eq("unpaired_no_mapped_incentive_counts").all()


def test_unmapped_incentive_year_preserves_its_actual_record_count_in_gap_audit(setup):
    bundle, inputs, _ = setup
    rows = inputs["composition_observations"]
    rows.loc[rows.source_kind.eq("izev"), "candidate_rating_classes"] = "Unknown class"
    result = benchmark.build_composition_benchmark(bundle, inputs)
    gap = result.coverage.loc[result.coverage.source.eq("izev") & result.coverage.year.eq(2024) & result.coverage.powertrain.eq("all")]
    assert gap.status.item() == "no_mapped_incentive_region_year"
    assert gap.total_count.item() == 13
    assert gap.mapped_count.item() == 0
    assert gap.coverage_share.item() == 0
    rows = result.comparison.loc[result.comparison.comparison_scope.str.startswith("new_pev")]
    assert rows.source.eq("statcan_new").all()
    assert not rows.counterpart_source_available.any()


def test_geographic_proxy_is_labelled_and_raw_native_labels_are_preserved(setup):
    bundle, inputs, _ = setup
    inputs["benchmark_statcan_new"]["geography"] = "Canada"
    result = benchmark.build_composition_benchmark(bundle, inputs)
    rows = result.observations.loc[result.observations.source.eq("statcan_new")]
    assert rows.status.eq("geography_proxy").all()
    assert rows.source_geography.eq("Canada").all()
    assert "Passenger cars" in set(rows.native_vehicle_class)
    assert "Battery electric" in set(rows.native_powertrain)


@pytest.mark.parametrize("value", [-1, .5, np.inf])
def test_invalid_source_counts_fail_before_comparison(setup, value):
    bundle, inputs, _ = setup
    inputs["benchmark_izev_records"] = inputs["benchmark_izev_records"].astype({"count": float})
    inputs["benchmark_izev_records"].loc[0, "count"] = value
    with pytest.raises(ValueError, match="nonnegative integer counts"):
        benchmark.build_composition_benchmark(bundle, inputs)


def test_missing_capture_requires_explicit_rebuild(setup):
    bundle, inputs, _ = setup
    del inputs["benchmark_statcan_stock"]
    with pytest.raises(ValueError, match="Rebuild notebook evidence"):
        benchmark.build_composition_benchmark(bundle, inputs)


def test_collector_only_returns_prepared_configured_paths(setup, monkeypatch, tmp_path):
    bundle, _, _ = setup
    monkeypatch.setattr(benchmark, "resolve_input_path", lambda _bundle, _layer, subdir: tmp_path / subdir)
    monkeypatch.setattr(benchmark, "resolve_artifact_path", lambda _bundle, family, filename: tmp_path / family / filename)
    paths = {"statcan/new.csv", "statcan/stock.csv", "vehicle_survival_interim/passenger.csv",
             "vehicle_survival_interim/commercial.csv", "road_aggregation/mapped.csv", "transport_canada_izev/izev.csv"}
    for name in paths:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("registered prepared fixture", encoding="utf-8")
    collected = benchmark.collect_composition_benchmark_inputs(bundle)
    assert {path.relative_to(tmp_path).as_posix() for path in collected.values()} == paths
    collected["benchmark_statcan_stock"].unlink()
    with pytest.raises(FileNotFoundError, match="Prepare the diagnostic benchmark inputs"):
        benchmark.collect_composition_benchmark_inputs(bundle)


@pytest.mark.parametrize('status,classes,expected_count',[
    ('resolved','Sport utility vehicle: Small | Sport utility vehicle: Standard',100),
    ('ambiguous_rating_class','Sport utility vehicle: Small | Sport utility vehicle: Standard',0),
    ('family_consumption_heterogeneity','Sport utility vehicle: Small | Sport utility vehicle: Standard',0),
    ('conflicting_reviewed_rating_class','Sport utility vehicle: Small | Sport utility vehicle: Standard',0),
    ('resolved','Compact | Sport utility vehicle: Small',0),
    ('resolved','Unknown class | Sport utility vehicle: Small',0),
])
def test_mto_catalogue_subset_respects_supported_predominant_class_and_final_disposition(setup,status,classes,expected_count):
    bundle,inputs,_=setup
    rows=inputs['composition_observations']
    selected=rows.source_kind.eq('mto') & rows.rating_class.eq('Sport utility vehicle: Small')
    rows.loc[selected,'candidate_rating_classes']=classes
    rows.loc[selected,'class_resolution_status']='predominant_distinct_target_year_configurations'
    rows.loc[selected,'match_status']=status
    result=benchmark.build_composition_benchmark(bundle,inputs)
    subset=result.comparison.loc[result.comparison.source.eq('mto_catalogue_subset')
        & result.comparison.vehicle_group.eq('Light trucks')]
    assert subset['count'].sum()==expected_count
    # Catalogue candidates remain intact; a resolved assignment never erases
    # the native class disagreement or creates another Ontario source count.
    assert rows.loc[selected,'candidate_rating_classes'].item()==classes
    cars=result.comparison.loc[result.comparison.source.eq('mto_catalogue_subset')
        & result.comparison.vehicle_group.eq('Cars')]
    assert cars['count'].sum()==50


def test_benchmark_uses_reviewed_powertrain_scoped_links_without_mixing_same_visible_model(setup, monkeypatch):
    bundle, _, spec = setup
    reference = reference_for([
        ("MINI", "Cooper SE", "MINI", "Cooper SE 3 Door", 2024, "bev"),
        ("MINI", "Cooper SE", "MINI", "Cooper", 2024, "phev"),
        ("Dodge", "Charger", "Dodge", "Charger Daytona", 2024, "bev"),
    ])
    monkeypatch.setattr(benchmark, "load_vehicle_reference", lambda captured: reference)
    raw = pd.DataFrame([{
        "province": "ON", "request_year": 2024, "model_year": 2024, "Purchase or Lease": "Purchase",
        "make": make, "model": model, "powertrain": powertrain, "count": count,
    } for make, model, powertrain, count in [
        ("MINI", "Cooper SE", "bev", 68), ("MINI", "Cooper SE", "phev", 55), ("Dodge", "Charger", "bev", 1),
    ]])
    matched = pd.DataFrame([{
        "source_kind": "izev", "year": 2024, "vehicle_id": identifier,
        "observed_powertrain": powertrain, "candidate_rating_classes": rating_class,
    } for identifier, powertrain, rating_class in [
        ("MINI/COOPERSE3DOOR", "bev", "Subcompact"),
        ("MINI/COOPER", "phev", ""), ("DODGE/CHARGERDAYTONA", "bev", "Full-size"),
    ]])
    original = raw.copy(deep=True)
    _, coverage, _, observations = benchmark._izev(bundle, raw, matched, spec, {"Subcompact": "Cars", "Full-size": "Cars"}, ["ON"])
    counts = coverage.set_index("powertrain")
    assert counts.loc["bev", "total_count"] == counts.loc["bev", "mapped_count"] == 69
    assert counts.loc["phev", "total_count"] == 55 and counts.loc["phev", "mapped_count"] == 0
    assert observations["count"].sum() == 124
    mini = observations.loc[observations.source_make.eq("MINI")].set_index("powertrain")
    assert mini.loc["bev", "vehicle_group"] == "Cars" and mini.loc["phev", "vehicle_group"] == ""
    assert mini.source_model.eq("Cooper SE").all()
    pd.testing.assert_frame_equal(raw, original)


def test_source_link_year_uses_original_model_year_before_request_year(setup, monkeypatch):
    bundle, _, spec = setup
    reference = reference_for([
        ("Fixture", "Shared label", "Fixture", "Car family", 2022, "bev"),
        ("Fixture", "Shared label", "Fixture", "Truck family", 2023, "bev"),
    ])
    monkeypatch.setattr(benchmark, "load_vehicle_reference", lambda captured: reference)
    raw = pd.DataFrame([{
        "province": "ON", "request_year": 2024, "model_year": year, "Purchase or Lease": "Lease",
        "make": "Fixture", "model": "Shared label", "powertrain": "bev", "count": count,
    } for year, count in [(2022, 4), (2023, 6)]])
    matched = pd.DataFrame([{
        "source_kind": "izev", "year": year, "vehicle_id": identifier,
        "observed_powertrain": "bev", "candidate_rating_classes": vehicle_class,
    } for year, identifier, vehicle_class in [
        (2022, "FIXTURE/CARFAMILY", "Compact"), (2023, "FIXTURE/TRUCKFAMILY", "Sport utility vehicle: Small"),
    ]])
    _, _, timing, observations = benchmark._izev(bundle, raw, matched, spec, {
        "Compact": "Cars", "Sport utility vehicle: Small": "Light trucks",
    }, ["ON"])
    assert observations.set_index("vehicle_group")["count"].to_dict() == {"Cars": 4, "Light trucks": 6}
    assert set(timing.request_year) == {2024} and set(timing.model_year) == {2022, 2023}


def test_native_duplicate_transactions_keep_counts_and_missing_rating_year_is_not_borrowed(setup):
    bundle, inputs, _ = setup
    raw = inputs["benchmark_izev_records"]
    inputs["benchmark_izev_records"] = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)
    observations = inputs["composition_observations"]
    observations.loc[observations.source_kind.eq("izev") & observations.family_model.eq("Model 3"), "year"] = 2024
    result = benchmark.build_composition_benchmark(bundle, inputs)
    native = result.observations.loc[result.observations.source.eq("izev")]
    assert native["count"].sum() == 16
    model3 = native.loc[native.source_model.eq("Model 3")]
    assert model3["count"].sum() == 7 and model3.vehicle_group.eq("").all()
    assert model3.status.eq("unmatched_or_ambiguous_class").all()
