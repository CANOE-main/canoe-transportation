"""Source distributions, class resolution and recorded efficiency bridge contracts."""

from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from parameterization.road_efficiencies import select_atb_consumption
from utils import load_config_bundle, load_conversion_factors, load_harmonization_rules
from validation import ldv_efficiency_diagnostic as diagnostic


@pytest.fixture
def bundle():
    loaded = load_config_bundle("config/scenarios/default.yaml")
    geography = loaded.scenario.geography.model_copy(update={"regions": ["ON"]})
    return replace(loaded, scenario=loaded.scenario.model_copy(update={"geography": geography}))


def ratings():
    return pd.DataFrame([
        {"included": True, "year": year, "rating_class": size, "powertrain": "bev_300_miles",
         "Make": "Maker", "Model": model, "native_consumption": value,
         "native_unit": "kWh/100 km", "consumption_mj_per_vkm": value * .036}
        for year, size, model, value in [(2023, "Compact", "A", 10), (2023, "Compact", "A", 20),
                                       (2023, "Compact", "B", 100), (2026, "Mid-size", "C", 30)]
    ])


def test_equal_catalogue_rows_expose_model_variant_influence(bundle):
    rows, summary = diagnostic.rating_distributions(bundle, ratings())
    compact = rows.loc[rows.rating_class.eq("Compact")]
    means = summary.loc[summary.rating_class.eq("Compact")].iloc[0]
    assert compact.equal_row_contribution.sum() == pytest.approx(1)
    assert set(compact.loc[compact.Model.eq("A"), "make_model_contribution"]) == {2 / 3}
    assert means.equal_row_mean == pytest.approx(130 / 3)
    assert means.equal_distinct_model_mean == pytest.approx((15 + 100) / 2)
    assert means.catalogue_rows == 3 and means.distinct_models == 2
    assert set(rows.atb_class) == {"Compact", "Midsize"}
    assert set(rows.vehicle_group) == {"Cars"}
    assert not rows.loc[rows.year.eq(2026), "calibration_observation"].any()


def test_phev_cs_observations_remain_native_indices_without_total_energy(bundle):
    source = ratings().iloc[[0]].assign(powertrain="phev_35_miles", native_unit="L/100 km (CS; index only)", consumption_mj_per_vkm=np.nan)
    rows, summary = diagnostic.rating_distributions(bundle, source)
    assert rows.consumption_mj_per_vkm.isna().all()
    assert summary.consumption_mj_per_vkm.isna().all()
    assert summary.equal_row_mean.iloc[0] == 10


def phev_ratings_and_weights(index_mode="CD"):
    rows, weights = [], []
    for year, multiplier in [(2020, 1.5), (2022, 1.2), (2023, 1), (2026, .5)]:
        for size, weight, cs, cd_values in [("Compact", .25, 4, [1, 3]), ("Subcompact", .75, 8, [6])]:
            cs_value = cs * (2 - multiplier)
            native_unit = "Le/100 km (CD; index only)" if index_mode == "CD" else "L/100 km (CS; index only)"
            weights.append({"year": year, "powertrain": "phev_35_miles", "rating_class": size,
                "ceud_class": "Car", "weight": weight, "native_class_mean": np.mean(cd_values) * multiplier if index_mode == "CD" else cs_value,
                "class_mean_mj_per_vkm": np.nan})
            for cd in cd_values:
                rows.append({"included": True, "year": year, "rating_class": size, "powertrain": "phev_35_miles",
                    "Make": "Maker", "Model": size, "native_consumption": cd * multiplier if index_mode == "CD" else cs_value,
                    "native_unit": native_unit, "consumption_mj_per_vkm": np.nan,
                    "combined_cs_consumption_L_100_km": cs_value,
                    "combined_cd_consumption_Le_100_km": cd * multiplier})
    rows.append({**rows[0], "included": False, "combined_cd_consumption_Le_100_km": 1000})
    return pd.DataFrame(rows), pd.DataFrame(weights)


def test_phev_components_preserve_operating_basis_and_recorded_class_weights(bundle):
    rows, weights = phev_ratings_and_weights()
    original = rows.copy(deep=True)
    result = diagnostic.phev_rating_components(bundle, rows, weights)
    selected = result.loc[result.year.eq(2023)].set_index("component")
    conversions = load_conversion_factors(bundle)
    assert selected.loc["CS", "native_value"] == pytest.approx(7)
    assert selected.loc["CD", "native_value"] == pytest.approx(5)
    assert selected.loc["CS", "value"] == pytest.approx(7 * conversions["fuel_hhv"]["gasoline_mj_per_litre"] / 100)
    assert selected.loc["CD", "value"] == pytest.approx(5 * conversions["energy"]["nrcan_kwh_per_litre_gasoline_equivalent"] * conversions["energy"]["kwh_to_mj"] / 100)
    assert set(result.component) == {"CS", "CD"} and not result.utility_weighted.any()
    assert "may include liquid fuel" in selected.loc["CD", "native_basis"]
    assert selected.loc["CD", "energy_basis"] != selected.loc["CS", "energy_basis"]
    pd.testing.assert_frame_equal(rows, original)
    assert rows.consumption_mj_per_vkm.isna().all()


def test_phev_component_comparison_rejects_missing_cd_class_and_invalid_values(bundle):
    rows, weights = phev_ratings_and_weights()
    with pytest.raises(ValueError, match="missing a recorded rating class"):
        diagnostic.phev_rating_components(bundle, rows.loc[rows.rating_class.eq("Compact")], weights)
    for invalid in [np.nan, np.inf, -1]:
        bad = rows.copy()
        bad.loc[0, "combined_cd_consumption_Le_100_km"] = invalid
        with pytest.raises(ValueError, match="finite non-negative"):
            diagnostic.phev_rating_components(bundle, bad, weights)


def test_cd_backcast_uses_observed_cohorts_and_exact_base_without_analogues(bundle):
    rows, weights = phev_ratings_and_weights()
    components = diagnostic.phev_rating_components(bundle, rows, weights)
    result = diagnostic.phev_component_backcast(components, component="CD", base_year=2023, anchor_consumption=2)
    assert result.year.tolist() == [2020, 2022, 2023]
    assert result.value.tolist() == pytest.approx([3, 2.4, 2])
    assert result.consumption_index.tolist() == pytest.approx([1.5, 1.2, 1])
    assert result.index_segment.tolist() == [1, 2, 2]
    missing_base = components.loc[~components.year.eq(2023)]
    assert diagnostic.phev_component_backcast(missing_base, component="CD", base_year=2023, anchor_consumption=2).empty
    with pytest.raises(ValueError, match="one class/powertrain cohort"):
        diagnostic.phev_component_backcast(pd.concat([components, components]), component="CD", base_year=2023, anchor_consumption=2)
    previous = diagnostic.phev_component_backcast(components, component="CS", base_year=2023, anchor_consumption=2)
    assert previous.value.tolist() == pytest.approx([1, 1.6, 2])


def test_native_phev_keeps_cs_and_cd_means_distinct(bundle, monkeypatch):
    rows, _ = phev_ratings_and_weights()
    distribution, _ = diagnostic.rating_distributions(bundle, rows)
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {})
    monkeypatch.setattr(diagnostic, "select_atb_consumption", lambda *args, **kwargs: pd.DataFrame())
    inputs = {"atb_phev": pd.DataFrame(columns=["trajectory"]), "atb_vehicles": pd.DataFrame(),
              "atb": pd.DataFrame(columns=["mode", "vehicle_class"])}
    native = diagnostic._raw_sources(bundle, inputs, distribution)
    selected = native.loc[native.year.eq(2023) & native.source_class.eq("Compact")].set_index("series")
    assert selected.loc["NRCan CS equal catalogue-row mean", "value"] == 4
    assert selected.loc["NRCan CD equal catalogue-row mean", "value"] == 2
    assert set(selected.units) == {"L/100 km (CS)", "Le/100 km (CD)"}


@pytest.mark.parametrize("index_mode", ["CS", "CD"])
def test_bridge_respects_recorded_index_and_places_phev_components_beside_atb_total(bundle, monkeypatch, index_mode):
    rows, weights = phev_ratings_and_weights(index_mode)
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Cars": "cars"})
    original_rules = diagnostic.load_harmonization_rules
    def rules(config, module):
        selected = original_rules(config, module)
        if module == "efficiencies":
            selected["ratings"]["phev_historical_index"] = f"combined_{index_mode.lower()}_consumption"
        return {**selected, "anchor_powertrains": {}} if module == "ldv_efficiency_diagnostic" else selected
    monkeypatch.setattr(diagnostic, "load_harmonization_rules", rules)
    class Evidence:
        def derive(self, region, mode, powertrain, year):
            return {"native_intensity": 2, "efficiency": 1, "treatment": "production unchanged",
                    "analogue": powertrain, "consumption_index": 1, "load_factor": 2}
        def atb_value(self, region, mode, powertrain, year):
            return 2
    monkeypatch.setattr(diagnostic, "_evidence", lambda *args: Evidence())
    inputs = {"ratings": rows, "rating_weights": weights,
        "atb": pd.DataFrame([{"region": "ON", "mode": "cars", "vehicle_class": "Compact", "year": 2023,
                              "powertrain": "phev_35_miles", "consumption_mj_per_vkm": 2, "weighted_consumption": 2}]),
        "loads": pd.DataFrame({"region": ["ON", "ON"], "mode": ["cars", "cars"], "year": [2020, 2023], "load_factor": [2, 2]})}
    bridge, _ = diagnostic._bridge_rows(bundle, inputs)
    harmonised = bridge.loc[bridge.stage.eq("Harmonised before indexing")]
    assert set(harmonised.series) == {"NRCan CS fuel-only aggregate", "NRCan CD equivalent-energy aggregate", "ATB absolute total energy"}
    alternative_label = "Previous NRCan CS backcast" if index_mode == "CD" else "NRCan CD backcast comparison"
    alternative = bridge.loc[bridge.series.eq(alternative_label) & bridge.metric.eq("consumption")]
    assert set(alternative.year) == {2020, 2022, 2023}
    assert alternative.loc[alternative.year.eq(2020), "value"].tolist() == pytest.approx([1, 1] if index_mode == "CD" else [3, 3])
    production = bridge.loc[bridge.series.eq("Production method") & bridge.metric.eq("consumption")]
    assert production.value.eq(2).all()
    assert set(production.loc[production.year.le(2023), "index_basis"]) == {f"NRCan {index_mode} / approved analogue"}


def audit():
    return pd.DataFrame([
        {"year": year, "powertrain": powertrain, "rating_class": size,
         "ceud_class": "Car", "weight": weight, "native_class_mean": mj / .036,
         "class_mean_mj_per_vkm": mj}
        for year in (2010, 2023, 2026)
        for powertrain in ("gasoline", "bev_300_miles")
        for size, weight, mj in (("Compact", .2, 100 if year == 2026 else 1),
                                 ("Subcompact", .3, 100 if year == 2026 else 2),
                                 ("Mid-size", .5, 100 if year == 2026 else 4))
    ])


def test_class_recalculation_renormalizes_only_within_atb_class(bundle):
    selected = diagnostic._class_rating_audit(bundle, audit(), "Compact")
    assert set(selected.rating_class) == {"Compact", "Subcompact"}
    assert set(selected.ceud_class) == {"Compact"}
    assert set(selected.loc[selected.rating_class.eq("Compact"), "weight"]) == {.4}
    assert set(selected.loc[selected.rating_class.eq("Subcompact"), "weight"]) == {.6}
    assert selected.groupby(["year", "powertrain"]).weight.sum().eq(1).all()


def test_future_rating_holdouts_never_calibrate_detailed_class_baseline(bundle):
    atb = pd.DataFrame([
        {"region": "ON", "mode": "cars", "vehicle_class": "Compact", "powertrain": powertrain,
         "year": year, "consumption_mj_per_vkm": value, "weighted_consumption": value * .2}
        for powertrain in ("gasoline", "bev_300_miles") for year, value in ((2023, 1), (2030, .8))
    ])
    inputs = {"atb": atb, "rating_weights": audit(), "ceud": pd.DataFrame(),
              "loads": pd.DataFrame({"region": ["ON", "ON"], "mode": ["cars", "cars"], "year": [2010, 2023], "load_factor": [2, 2]})}
    evidence = diagnostic._evidence(bundle, inputs, "cars", "Compact")
    assert evidence.ratings.year.max() == 2023
    assert evidence.ldv_baseline("ON", "cars", "bev_300_miles")[0] == pytest.approx(1.6)
    assert evidence.derive("ON", "cars", "bev_300_miles", 2030)["native_intensity"] == pytest.approx(1.6 * .8)


def test_sole_anchor_preserves_atb_range_penalty_and_level():
    short = diagnostic.sole_anchor_consumption(.72, .5, .6)
    anchor = diagnostic.sole_anchor_consumption(.72, .6, .6)
    long = diagnostic.sole_anchor_consumption(.72, .7, .6)
    assert anchor == pytest.approx(.72)
    assert long / short == pytest.approx(.7 / .5)
    assert short < anchor < long
    with pytest.raises(ValueError, match="finite and positive"):
        diagnostic.sole_anchor_consumption(.72, .5, 0)


def test_existing_representative_conserves_weighted_consumption():
    consumption = diagnostic.representative_consumption({"short": .5, "long": 1}, {"short": .25, "long": .75})
    assert consumption == pytest.approx(.875)
    load = 2
    assert load / consumption == pytest.approx(1 / (.25 / (load / .5) + .75 / (load / 1)))
    for shares in ({"short": .3, "long": .8}, {"missing": 1}, {"short": -1, "long": 2}):
        with pytest.raises(ValueError, match="Incomplete or unnormalised"):
            diagnostic.representative_consumption({"short": .5, "long": 1}, shares)


def test_source_atb_conversion_uses_same_electricity_and_hhv_contract(bundle):
    rules = load_harmonization_rules(bundle, "efficiencies")
    rules["ldv_archetypes"] = {key: rules["ldv_archetypes"][key] for key in ("gasoline", "bev_300_miles")}
    rules["mhdv_archetypes"] = {}
    frame = pd.DataFrame([
        {"trajectory": "Mid", "vehicle_weight_category": "Light Duty", "vehicle_detail": detail,
         "vehicle_class": "Compact", "year": 2023, "metric_key": "fuel_economy_mi_per_gge", "unit": "mi/gge", "value": value}
        for detail, value in ((rules["ldv_archetypes"]["gasoline"]["detail"], 40), (rules["ldv_archetypes"]["bev_300_miles"]["detail"], 120))
    ])
    conversions = load_conversion_factors(bundle)
    selected = select_atb_consumption(frame, pd.DataFrame(), trajectory="Mid", rules=rules, conversions=conversions).set_index("powertrain")
    expected_gas = conversions["fuel_hhv"]["gasoline_mj_per_litre"] * conversions["volume"]["us_gallon_to_litre"] / 40 / conversions["length"]["mile_to_km"]
    expected_bev = conversions["energy"]["wh_per_gge"] * conversions["energy"]["wh_to_mj"] / 120 / conversions["length"]["mile_to_km"]
    assert selected.loc["gasoline", "consumption_mj_per_vkm"] == pytest.approx(expected_gas)
    assert selected.loc["bev_300_miles", "consumption_mj_per_vkm"] == pytest.approx(expected_bev)
    assert selected.loc["bev_300_miles", "source_value"] == 120


def test_final_csv_keeps_vintage_and_source_year_separate(bundle):
    inputs = {"final_efficiency": pd.DataFrame([
        {"tech": "T_EX", "region": "ON", "vintage": 2020, "efficiency": 2, "units": "bn passenger-km/PJ"},
        {"tech": "T_N", "region": "ON", "vintage": 2025, "efficiency": 3, "units": "bn passenger-km/PJ"},
    ]), "relationships": pd.DataFrame({"tech": ["T_EX", "T_N"], "mode": ["cars", "cars"], "powertrain": ["bev", "bev_300_miles"]})}
    periods = diagnostic._published_periods(bundle, inputs)
    assert periods.set_index("vintage").year.to_dict() == {2020: 2023, 2025: 2030}
    assert periods.value.tolist() == [2, 3]
    assert "Mean annual" in periods.time_basis.iloc[0]
    assert "interval-end" in periods.time_basis.iloc[1]


@pytest.mark.parametrize("suffix", [".csv", ".csv.gz"])
def test_loader_uses_recorded_snapshot_and_never_accepts_reader_scenario(bundle, tmp_path, monkeypatch, suffix):
    from validation import notebook_evidence

    source = tmp_path / ("actual" + suffix)
    pd.DataFrame({"value": [3]}).to_csv(source, index=False)
    metadata = {"build_id": "recorded-build", "built_at": "2026-10-17", "scenario": {"name": "recorded"}}
    recorded = SimpleNamespace(bundle=bundle, paths={"actual": source}, metadata=metadata)
    seen = []
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda kind, repo_root: seen.append((kind, repo_root)) or recorded)
    monkeypatch.setattr(diagnostic, "build_diagnostic_frames", lambda config, inputs: ({"actual": inputs["actual"]}, {"base_year": config.scenario.periods.base_year}))
    monkeypatch.setattr(diagnostic, "write_dataframe_atomic", lambda *args: pytest.fail("loader wrote evidence"))
    loaded = diagnostic.load_diagnostic(repo_root=tmp_path)
    assert seen == [("efficiency", tmp_path)]
    assert loaded.frames["actual"].value.iloc[0] == 3
    assert loaded.summary["build"] == metadata
    assert loaded.summary["inputs"]["actual"]["path"] == str(source)
    with pytest.raises(TypeError):
        diagnostic.load_diagnostic("config/scenarios/default.yaml")


def test_collector_captures_raw_final_and_benchmark_evidence(bundle):
    paths = diagnostic.collect_efficiency_inputs(bundle)
    assert {"atb_vehicles", "atb_phev", "ceud", "final_efficiency", "final_splits", "loads", "relationships",
            "benchmark_matches", "benchmark_allocations", "benchmark_weighted", "omega_records", "omega_buckets"} <= set(paths)
    outputs = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["outputs"]
    assert {"raw_ratings_" + component for component in outputs} <= set(paths)


def test_future_truck_powertrains_do_not_require_unobserved_historical_labels(bundle, monkeypatch):
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Heavy trucks": "heavy_trucks"})
    base = bundle.scenario.periods.base_year
    class Evidence:
        def derive(self, region, mode, powertrain, year):
            if year <= base and powertrain != "diesel":
                pytest.fail("Unobserved electric history was invented")
            consumption = 2 if powertrain == "diesel" else .5
            return {"native_intensity": consumption, "efficiency": 1 / consumption,
                    "treatment": "source projection", "analogue": powertrain,
                    "consumption_index": 1, "load_factor": 1}
    monkeypatch.setattr(diagnostic, "_evidence", lambda *args: Evidence())
    monkeypatch.setattr(diagnostic, "harmonised_ceud_history", lambda *args: pd.DataFrame())
    atb = pd.DataFrame([
        {"region": "ON", "mode": "heavy_trucks", "vehicle_class": "Sleeper",
         "powertrain": powertrain, "year": year, "weighted_consumption": value,
         "consumption_mj_per_vkm": value}
        for year in [base, base + 7] for powertrain, value in [("diesel", 2), ("bev", .5)]
    ])
    bridge, _ = diagnostic._bridge_rows(bundle, {"atb": atb, "rating_weights": audit(), "ceud": pd.DataFrame()})
    indexed = bridge.loc[bridge.stage.eq("Indexed bridge") & bridge.powertrain.eq("bev")]
    assert not indexed.empty and indexed.year.min() == base + 1
    assert set(indexed.atb_class) == {diagnostic.COHORT, "Sleeper"}
    assert indexed.loc[indexed.metric.eq("consumption"), "value"].eq(.5).all()


def test_ceud_pre_index_energy_keeps_native_fuel_ratio(bundle):
    rules = load_harmonization_rules(bundle, "efficiencies")
    road = rules["road_classes"]["heavy_trucks"]
    ceud = pd.DataFrame([
        {"table_id": road["stock_table"], "raw_series": road["cs_prefix"] + "|" + rules["fuel_series"]["diesel"],
         "unit": "L/100 km", "year": year, "value": value}
        for year, value in [(2010, 10), (bundle.scenario.periods.base_year, 20)]
    ])
    result = diagnostic.harmonised_ceud_history(bundle, ceud, "heavy_trucks").set_index("year")
    hhv = load_conversion_factors(bundle)["fuel_hhv"]["diesel_mj_per_litre"]
    assert result.loc[2010, "value"] == pytest.approx(10 * hhv / 100)
    assert result.loc[2010, "consumption_index"] == pytest.approx(.5)
    assert result.loc[bundle.scenario.periods.base_year, "consumption_index"] == 1
    assert set(result.source_fuel) == {"diesel"}


def test_benchmark_distinguishes_uncovered_fallback_from_resolved_stock(bundle):
    catalogue = audit().assign(aggregation_method="catalogue_class_weighted", cohort_matched_stock=0)
    weighted = catalogue.assign(aggregation_method="catalogue_fallback_no_resolved_stock")
    weighted.loc[weighted.year.eq(2023), "aggregation_method"] = "mto_stock_weighted"
    weighted.loc[weighted.year.eq(2023), "cohort_matched_stock"] = 10
    comparison = diagnostic._benchmark(bundle, {"rating_weights": catalogue, "benchmark_weighted": weighted})
    measured = comparison.loc[comparison.series.str.startswith("Diagnostic")]
    assert measured.loc[measured.year.eq(2023), "cohort_matched_stock"].eq(10).all()
    assert measured.loc[measured.year.eq(2010), "aggregation_method"].eq("catalogue_fallback_no_resolved_stock").all()
    assert measured.loc[measured.year.eq(2010), "cohort_matched_stock"].eq(0).all()
