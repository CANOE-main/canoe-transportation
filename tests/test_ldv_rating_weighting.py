"""Pure MTO-to-rating matching and stock-mass conservation checks."""

import pandas as pd
import pytest
from types import SimpleNamespace

from utils import file_sha256
from validation import rating_stock_benchmark as benchmark
from validation.rating_stock_benchmark import derive_rating_stock_weights


def policy(**overrides):
    return {
        "variant_allocation": "equal_distinct_variants",
        "rating_variant_identity_columns": ["Model", "native_consumption", "range_km"],
        "weight_sum_tolerance": 1e-9,
        **overrides,
    }


def stock(model="Alpha", count=90, year=2023, **overrides):
    return {
        "report_year": 2025, "MODEL_YEAR": year, "MAKE": "TEST", "MODEL": model,
        "FIT_ACTIVE": count, "mapping_accepted": True, "mapping_outcome": "mapped_ldv",
        "canonical_make": "Test", "canonical_model": model,
        "nrcan_vehicle_class": "Compact", "nlr_atb_class": "Compact", "nrcan_ceud_class": "Car",
        **overrides,
    }


def rating(row, model="Alpha", consumption=2.0, range_km=450.0, **overrides):
    return {
        "component": "battery_electric", "source_row": row, "year": 2023,
        "Model year": 2023, "Make": "Test", "Model": model, "Vehicle class": "Compact",
        "rating_class": "Compact", "canonical_make": "Test", "canonical_model": model,
        "powertrain": "bev_300_miles", "included": True,
        "native_consumption": consumption, "consumption_mj_per_vkm": consumption * 0.036,
        "native_unit": "kWh/100 km", "range_km": range_km,
        **overrides,
    }


def weighted_value(allocations, column):
    return (allocations[column] * allocations.allocated_fit_active_stock).sum() / allocations.allocated_fit_active_stock.sum()


def test_unequal_model_stock_weights_consumption_and_range_without_variant_count_bias():
    ratings = pd.DataFrame([
        rating(1, consumption=2, range_km=450), rating(2, consumption=4, range_km=500),
        rating(3, model="Beta", consumption=8, range_km=550),
    ])
    stocks = pd.DataFrame([stock(count=90), stock(model="Beta", count=10)])
    before_ratings, before_stocks = ratings.copy(deep=True), stocks.copy(deep=True)

    dispositions, allocations = derive_rating_stock_weights(ratings, stocks, rules=policy())

    assert dispositions.match_status.tolist() == ["resolved", "resolved"]
    assert allocations.groupby("mto_stock_row_id").allocated_fit_active_stock.sum().tolist() == [90, 10]
    assert weighted_value(allocations, "native_consumption") == pytest.approx(3.5)
    assert weighted_value(allocations, "range_km") == pytest.approx(482.5)
    assert allocations.rating_variant_share.tolist() == [0.5, 0.5, 1]
    assert set(allocations.variant_allocation) == {"equal_distinct_variants"}
    pd.testing.assert_frame_equal(ratings, before_ratings)
    pd.testing.assert_frame_equal(stocks, before_stocks)


def test_catalogue_duplication_preserves_variant_mass_and_source_provenance():
    original = pd.DataFrame([rating(1, consumption=2), rating(2, consumption=4)])
    duplicated = pd.concat([
        original, pd.DataFrame([rating(3, consumption=2)]), original.iloc[[0]],
    ], ignore_index=True)
    stocks = pd.DataFrame([stock(count=90)])

    dispositions, allocations = derive_rating_stock_weights(duplicated, stocks, rules=policy())

    assert dispositions.candidate_variant_count.item() == 2
    assert dispositions.candidate_rating_rows.item() == 3
    assert allocations.source_row.tolist() == [1, 2, 3]
    assert allocations.source_duplicate_count.tolist() == [2, 1, 1]
    assert allocations.allocated_fit_active_stock.tolist() == [22.5, 45, 22.5]
    assert allocations.groupby("rating_variant_id").allocated_fit_active_stock.sum().tolist() == [45, 45]
    assert weighted_value(allocations, "native_consumption") == pytest.approx(3)
    assert "battery_electric:3" in dispositions.candidate_rating_ids.item()


def test_exact_vintage_class_and_canonical_make_are_required():
    ratings = pd.DataFrame([
        rating(1, consumption=2),
        rating(2, consumption=80, year=2022, **{"Model year": 2022}),
        rating(3, consumption=90, rating_class="Mid-size", **{"Vehicle class": "Mid-size"}),
        rating(4, consumption=100, canonical_make="Other", Make="Other"),
    ])
    stocks = pd.DataFrame([stock(), stock(year=2021), stock(nrcan_vehicle_class="Subcompact")])

    dispositions, allocations = derive_rating_stock_weights(ratings, stocks, rules=policy())

    assert dispositions.match_status.tolist() == ["resolved", "unmatched_ratings", "unmatched_ratings"]
    assert allocations.source_row.tolist() == [1]
    assert allocations.allocated_fit_active_stock.item() == 90


def test_propulsion_and_range_ambiguity_remain_unallocated_with_complete_stock_dispositions():
    stocks = pd.DataFrame([
        stock(model="Mixed", count=100), stock(model="Ranges", count=50),
        stock(model="Resolved", count=30), stock(model="Missing", count=20),
        stock(model="Resolved", count=10, mapping_accepted=False, mapping_outcome="unmapped"),
    ])
    ratings = pd.DataFrame([
        rating(1, model="Mixed", powertrain="gasoline"), rating(2, model="Mixed", powertrain="hev"),
        rating(3, model="Ranges", powertrain="bev_200_miles"), rating(4, model="Ranges"),
        rating(5, model="Resolved", range_km=450), rating(6, model="Resolved", range_km=500),
    ])

    dispositions, allocations = derive_rating_stock_weights(ratings, stocks, rules=policy())

    assert dispositions.match_status.tolist() == [
        "ambiguous_powertrain", "ambiguous_range_bucket", "resolved", "unmatched_ratings", "mapping_not_accepted",
    ]
    assert dispositions.candidate_powertrain_family_count.tolist() == [2, 1, 1, 0, 1]
    assert dispositions.resolved_powertrain.tolist() == ["", "", "bev_300_miles", "", ""]
    assert dispositions.mto_stock_row_id.tolist() == list(range(len(stocks)))
    assert (dispositions.matched_fit_active_stock + dispositions.unresolved_fit_active_stock).tolist() == stocks.FIT_ACTIVE.tolist()
    assert dispositions.unresolved_fit_active_stock.sum() == 180
    assert allocations.allocated_fit_active_stock.sum() == 30
    assert allocations.mto_stock_row_id.unique().tolist() == [2]
    assert dispositions.mto_nrcan_ceud_class.tolist() == dispositions.nrcan_ceud_class.tolist()


def test_existing_precomputed_family_catalogue_attaches_raw_variant_labels():
    ratings = pd.DataFrame([rating(1, model="Alpha AWD", consumption=2)]).drop(columns=["canonical_make", "canonical_model"])
    catalogue = pd.DataFrame([{
        "Model year": 2023, "Make": "Test", "Model": "Alpha AWD", "Vehicle class": "Compact",
        "canonical_make": "Test", "canonical_model": "Alpha",
        "evidence_source": "nrcan_fuel_consumption_ratings",
    }])
    catalogue = pd.concat([catalogue, catalogue.assign(evidence_source="fueleconomy_gov_vehicle_data")])

    dispositions, allocations = derive_rating_stock_weights(
        ratings, pd.DataFrame([stock()]), rules=policy(), model_catalog=catalogue,
    )

    assert dispositions.match_status.item() == "resolved"
    assert allocations.Model.item() == "Alpha AWD"
    assert allocations.canonical_model.item() == "Alpha"
    assert allocations.mto_canonical_model.item() == "Alpha"


def test_excluded_ratings_produce_empty_allocations_with_complete_unmatched_stock():
    dispositions, allocations = derive_rating_stock_weights(
        pd.DataFrame([rating(1, included=False)]), pd.DataFrame([stock()]), rules=policy(),
    )

    assert dispositions.match_status.item() == "unmatched_ratings"
    assert dispositions.candidate_variant_count.item() == 0
    assert dispositions.unresolved_fit_active_stock.item() == 90
    assert allocations.empty
    assert {"allocated_fit_active_stock", "rating_variant_id", "range_km", "source_row"}.issubset(allocations)


def test_duplicate_source_identity_with_conflicting_values_fails():
    with pytest.raises(ValueError, match="Conflicting values.*source identity"):
        derive_rating_stock_weights(
            pd.DataFrame([rating(1), rating(1, consumption=4)]), pd.DataFrame([stock()]), rules=policy(),
        )


def test_variant_identity_cannot_conceal_consumption_or_range_differences():
    with pytest.raises(ValueError, match="variant identity merges conflicting"):
        derive_rating_stock_weights(
            pd.DataFrame([rating(1), rating(2, consumption=4)]), pd.DataFrame([stock()]),
            rules=policy(rating_variant_identity_columns=["Model"]),
        )


@pytest.mark.parametrize("count", [-1, float("nan"), pd.NA, float("inf"), 1.5])
def test_invalid_stock_counts_fail_before_allocation(count):
    with pytest.raises(ValueError, match="fit-active counts"):
        derive_rating_stock_weights(
            pd.DataFrame([rating(1)]), pd.DataFrame([stock(count=count)]), rules=policy(),
        )


@pytest.fixture
def prepared_benchmark(tmp_path, monkeypatch):
    routes = {
        name: tmp_path / name
        for name in ("efficiencies_interim", "efficiencies_processed", "road_aggregation", "rating_stock_benchmark")
    }
    for path in routes.values():
        path.mkdir()
    rules = {
        "stock_weighting": {
            **policy(), "method": "resolved_mto_model_year_stock",
            "unresolved_stock": "exclude_and_report", "uncovered_cohort": "catalogue_class_weighted",
        },
        "files": {"matches": "matches.csv", "allocations": "allocations.csv", "weighted": "weighted.csv"},
    }
    modules = {
        "rating_stock_benchmark": rules,
        "efficiencies": {"files": {"ratings": "classified.csv", "rating_weights": "catalogue.csv"}},
        "road_aggregation": {"mapped_current_stock_file": "mapped.csv"},
    }
    ratings = pd.DataFrame([
        rating(1, consumption=2), rating(2, consumption=4),
        rating(3, model="Mixed", powertrain="gasoline"),
        rating(4, model="Mixed", powertrain="hev"),
    ])
    ratings.to_csv(routes["efficiencies_interim"] / "classified.csv", index=False)
    catalogue = pd.DataFrame([{
        "ceud_class": "Car", "year": 2023, "powertrain": "bev_300_miles",
        "rating_class": "Compact", "native_class_mean": 3.0,
        "class_mean_mj_per_vkm": 0.108, "native_unit": "kWh/100 km",
        "observations": 2, "original_weight": 1, "observed_weight_sum": 1,
        "weight": 1, "range_class_mean_km": 450, "matched_stock": 0,
        "cohort_matched_stock": 0, "aggregation_method": "catalogue_class_weighted",
    }])
    catalogue.to_csv(routes["efficiencies_interim"] / "catalogue.csv", index=False)
    pd.DataFrame([stock(count=90), stock(model="Mixed", count=10)]).to_csv(
        routes["road_aggregation"] / "mapped.csv", index=False,
    )
    canonical = ratings[["Model year", "Make", "Model", "Vehicle class", "canonical_make", "canonical_model"]]
    monkeypatch.setattr(benchmark, "load_harmonization_rules", lambda _bundle, key: modules[key])
    monkeypatch.setattr(benchmark, "resolve_artifact_path", lambda _bundle, key: routes[key])
    monkeypatch.setattr(benchmark, "load_rating_evidence", lambda _bundle, **_kwargs: canonical)
    bundle = SimpleNamespace(scenario=SimpleNamespace(existing_capacity=SimpleNamespace(vehicle_population_year=2025)))
    return bundle, routes


def test_opt_in_benchmark_preserves_production_sources_and_reports_unresolved_stock(prepared_benchmark):
    bundle, routes = prepared_benchmark
    sources = [path for key, root in routes.items() if key != "rating_stock_benchmark" for path in root.glob("*.csv")]
    before = {path: file_sha256(path) for path in sources}

    paths = benchmark.build_rating_stock_benchmark(bundle)

    assert set(paths) == {"matches", "allocations", "weighted"}
    assert all(path.parent == routes["rating_stock_benchmark"] for path in paths.values())
    assert {path: file_sha256(path) for path in sources} == before
    matches, allocations, weighted = (pd.read_csv(paths[key]) for key in ("matches", "allocations", "weighted"))
    assert matches.match_status.tolist() == ["resolved", "ambiguous_powertrain"]
    assert matches.matched_fit_active_stock.sum() == allocations.allocated_fit_active_stock.sum() == 90
    assert matches.unresolved_fit_active_stock.sum() == 10
    assert weighted.native_class_mean.item() == 3
    assert weighted.matched_stock.item() == 90
    assert weighted.aggregation_method.item() == "mto_stock_weighted"


def test_benchmark_rejects_stale_promoted_stock_weighting(prepared_benchmark):
    bundle, routes = prepared_benchmark
    path = routes["efficiencies_interim"] / "catalogue.csv"
    pd.read_csv(path).assign(aggregation_method="mto_stock_weighted").to_csv(path, index=False)
    with pytest.raises(ValueError, match="production catalogue/class"):
        benchmark.build_rating_stock_benchmark(bundle)
    assert not list(routes["rating_stock_benchmark"].iterdir())


def test_benchmark_rejects_selected_edition_mismatch(prepared_benchmark):
    bundle, routes = prepared_benchmark
    bundle.scenario.existing_capacity.vehicle_population_year = 2026
    with pytest.raises(ValueError, match="stock edition differs"):
        benchmark.build_rating_stock_benchmark(bundle)
    assert not list(routes["rating_stock_benchmark"].iterdir())


def test_benchmark_rejects_output_route_overlapping_production(prepared_benchmark):
    bundle, routes = prepared_benchmark
    routes["rating_stock_benchmark"] = routes["efficiencies_interim"]
    with pytest.raises(ValueError, match="must not overwrite production"):
        benchmark.build_rating_stock_benchmark(bundle)
    assert sorted(path.name for path in routes["efficiencies_interim"].iterdir()) == ["catalogue.csv", "classified.csv"]


def test_benchmark_missing_prerequisite_does_not_build_or_write(prepared_benchmark):
    bundle, routes = prepared_benchmark
    (routes["efficiencies_interim"] / "classified.csv").unlink()
    with pytest.raises(FileNotFoundError, match="diagnostic scenario prerequisites"):
        benchmark.build_rating_stock_benchmark(bundle)
    assert not list(routes["rating_stock_benchmark"].iterdir())
