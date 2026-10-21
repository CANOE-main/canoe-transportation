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
                                       (2023, "Compact", "B", 100), (2023, "Compact", "C", 40),
                                       (2026, "Mid-size", "D", 30)]
    ])


def test_equal_catalogue_rows_expose_model_variant_influence(bundle):
    rows, summary = diagnostic.rating_distributions(bundle, ratings())
    compact = rows.loc[rows.rating_class.eq("Compact")]
    means = summary.loc[summary.rating_class.eq("Compact")].iloc[0]
    assert compact.equal_row_contribution.sum() == pytest.approx(1)
    assert set(compact.loc[compact.Model.eq("A"), "make_model_contribution"]) == {.5}
    assert means.equal_row_mean == pytest.approx(170 / 4)
    assert means.equal_distinct_model_mean == pytest.approx((15 + 100 + 40) / 3)
    assert means.equal_distinct_model_median == 40
    assert means["median"] == 30
    assert means.catalogue_rows == 4 and means.distinct_models == 3
    assert set(rows.atb_class) == {"Compact", "Midsize"}
    assert set(rows.vehicle_group) == {"Cars"}
    assert not rows.loc[rows.year.eq(2026), "calibration_observation"].any()


def test_phev_cd_observations_remain_native_indices_without_total_energy(bundle):
    source = ratings().iloc[[0]].assign(powertrain="phev_35_miles", native_unit="Le/100 km (CD; index only)", consumption_mj_per_vkm=np.nan)
    rows, summary = diagnostic.rating_distributions(bundle, source)
    assert rows.consumption_mj_per_vkm.isna().all()
    assert summary.consumption_mj_per_vkm.isna().all()
    assert summary.equal_row_mean.iloc[0] == 10


def phev_ratings_and_weights():
    rows, weights = [], []
    for year, multiplier in [(2020, 1.5), (2022, 1.2), (2023, 1), (2026, .5)]:
        for size, weight, cs, cd_values in [("Compact", .25, 4, [1, 3]), ("Subcompact", .75, 8, [6])]:
            cs_value = cs * (2 - multiplier)
            weights.append({"year": year, "powertrain": "phev_35_miles", "rating_class": size,
                "ceud_class": "Car", "weight": weight, "native_class_mean": np.mean(cd_values) * multiplier,
                "class_mean_mj_per_vkm": np.nan})
            for cd in cd_values:
                rows.append({"included": True, "year": year, "rating_class": size, "powertrain": "phev_35_miles",
                    "Make": "Maker", "Model": size, "native_consumption": cd * multiplier,
                    "native_unit": "Le/100 km (CD; index only)", "consumption_mj_per_vkm": np.nan,
                    "combined_cs_consumption_L_100_km": cs_value,
                    "combined_cd_consumption_Le_100_km": cd * multiplier})
    rows.append({**rows[0], "included": False, "combined_cd_consumption_Le_100_km": 1000})
    return pd.DataFrame(rows), pd.DataFrame(weights)


def bridge_support(bundle, mode, powertrains, *, load=1):
    periods = bundle.scenario.periods
    bins = {v: periods.historical_years(v) for v in periods.existing}
    bins.update({v: [periods.projection_year(v, legacy_at_end=True)] for v in periods.model})
    return pd.DataFrame([
        {"region": "ON", "mode": mode, "powertrain": powertrain, "vintage": vintage,
         "source_year": year, "load_factor": load, "temporal_weight": 1 / len(years)}
        for powertrain in powertrains for vintage, years in bins.items() for year in years
    ])


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


def test_native_phev_keeps_cs_and_cd_means_distinct(bundle, monkeypatch):
    rows, _ = phev_ratings_and_weights()
    distribution, _ = diagnostic.rating_distributions(bundle, rows)
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {})
    monkeypatch.setattr(diagnostic, "select_atb_consumption", lambda *args, **kwargs: pd.DataFrame())
    inputs = {"atb_phev": pd.DataFrame(columns=["trajectory"]), "atb_vehicles": pd.DataFrame(),
              "atb": pd.DataFrame(columns=["mode", "vehicle_class"])}
    native = diagnostic._raw_sources(bundle, inputs, distribution)
    selected = native.loc[native.year.eq(2023) & native.source_class.eq("Compact")].set_index("series")
    assert selected.loc["NRCan CS distinct-model mean", "value"] == 4
    assert selected.loc["NRCan CD distinct-model mean", "value"] == 2
    assert set(selected.units) == {"L/100 km (CS)", "Le/100 km (CD)"}


def test_bridge_places_phev_components_beside_atb_total_without_deprecated_alternatives(bundle, monkeypatch):
    rows, weights = phev_ratings_and_weights()
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Cars": "cars"})
    original_rules = diagnostic.load_harmonization_rules
    def rules(config, module):
        selected = original_rules(config, module)
        if module == "efficiencies":
            return {**selected, "pev_historical_anchors": {"phev": "phev_35_miles"}}
        return selected
    monkeypatch.setattr(diagnostic, "load_harmonization_rules", rules)
    class Evidence:
        def __init__(self, first_year):
            self.first_year = first_year
        def derive_consumption(self, region, mode, powertrain, year, *, allow_analogue=False):
            if year < self.first_year:
                return None
            return {"native_intensity": 2, "native_unit": "MJ/vkm", "treatment": "ATB_PEV_level_NRCan_shared_anchor_history",
                    "analogue": powertrain, "consumption_index": 1, "load_factor": 2,
                    "historical_index_anchor": "phev_35_miles"}
        def period_observations(self, region, mode, powertrain, years, *, allow_analogue_when_empty=False):
            return {y: self.derive_consumption(region, mode, powertrain, y) for y in years if y >= self.first_year}
        def atb_value(self, region, mode, powertrain, year):
            return 2
    monkeypatch.setattr(diagnostic, "_evidence", lambda config, inputs, mode, atb_class: Evidence(2020 if atb_class else 2000))
    monkeypatch.setattr(diagnostic, "_cohort_counts", lambda config, inputs, mode, powertrain, vintage:
                        pd.Series(1.0, index=bundle.scenario.periods.historical_years(vintage)))
    inputs = {"ratings": rows, "rating_weights": weights, "ceud": pd.DataFrame({"year": [2000]}),
        "atb": pd.DataFrame([{"region": "ON", "mode": "cars", "vehicle_class": "Compact", "year": 2023,
                              "powertrain": "phev_35_miles", "consumption_mj_per_vkm": 2, "weighted_consumption": 2}]),
        "loads": pd.DataFrame({"region": ["ON", "ON"], "mode": ["cars", "cars"], "year": [2020, 2023], "load_factor": [2, 2]}),
        "omega_buckets": pd.DataFrame([{"market_class": "car", "powertrain": "PHEV", "bucket": "phev_35_miles", "share_of_all_sales": 1}]),
        "annual": bridge_support(bundle, "cars", ["phev_35_miles"], load=2)}
    bridge, anchors = diagnostic._bridge_rows(bundle, inputs)
    harmonised = bridge.loc[bridge.stage.eq("Harmonised before indexing")]
    assert set(harmonised.series) == {"NRCan CS fuel-only aggregate", "NRCan CD equivalent-energy aggregate", "ATB absolute total energy"}
    assert not bridge.series.str.contains("backcast|Exploratory", case=False).any()
    assert not bridge.sample_kind.eq("curve").any()
    production = bridge.loc[bridge.series.eq("Production method") & bridge.metric.eq("consumption")]
    assert production.value.tolist() == pytest.approx([2] * len(production))
    assert set(production.sample_kind) == {"period", "base_year_anchor"}
    missing = bridge.loc[bridge.stage.eq("Missing class coverage")]
    assert not missing.empty and missing.atb_class.ne(diagnostic.COHORT).all()
    assert missing.value.isna().all()
    assert missing.treatment.str.contains("no interpolated period marker").all()
    detailed = production.loc[production.atb_class.ne(diagnostic.COHORT) & production.sample_kind.eq("period")]
    assert detailed.year.min() >= 2020
    annual = bridge.loc[bridge.stage.eq("Annual calculation")]
    assert not annual.empty and set(annual.historical_index_anchor) == {"phev_35_miles"}
    assert set(anchors.anchor) == {"phev_35_miles"}


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


def test_post_base_ratings_never_calibrate_detailed_class_baseline(bundle):
    atb = pd.DataFrame([
        {"region": "ON", "mode": "cars", "vehicle_class": "Compact", "powertrain": powertrain,
         "year": year, "consumption_mj_per_vkm": value, "weighted_consumption": value * .2}
        for powertrain in ("gasoline", "bev_300_miles") for year, value in ((2023, 1), (2030, .8))
    ])
    inputs = {"atb": atb, "rating_weights": audit(), "ceud": pd.DataFrame(),
              "loads": pd.DataFrame({"region": ["ON", "ON"], "mode": ["cars", "cars"], "year": [2010, 2023], "load_factor": [2, 2]})}
    evidence = diagnostic._evidence(bundle, inputs, "cars", "Compact")
    assert evidence.ratings.year.max() == 2023
    assert evidence.ldv_baseline("ON", "cars", "gasoline")[0] == pytest.approx(1.6)
    assert evidence.derive_consumption("ON", "cars", "bev_300_miles", 2030)["native_intensity"] == pytest.approx(.8)


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


def prepared_period_inputs():
    final = pd.DataFrame([
        {"tech": "T_C_BEV_EX", "region": "ON", "vintage": 2020, "efficiency": 2, "units": "bn passenger-km/PJ"},
        {"tech": "T_C_BEV300_N", "region": "ON", "vintage": 2025, "efficiency": 3, "units": "bn passenger-km/PJ"},
        {"tech": "T_MDV_T_BEV_EX", "region": "ON", "vintage": 2020, "efficiency": .8, "units": "bn tonne-km/PJ"},
    ]).assign(input_comm="electricity", output_comm="service")
    periods = final[["region", "tech", "vintage", "input_comm", "output_comm"]].copy()
    periods["native_intensity"] = [2.35, 2 / 3, 5]
    periods["load_factor"] = [4.7, 2, 4]
    periods["native_unit"] = "MJ/vkm"
    periods["weighted_source_year"] = [2022.6, 2030, 2023]
    periods["source_year_start"] = [2021, 2030, 2023]
    periods["source_year_end"] = [2023, 2030, 2023]
    periods["candidate_year_count"] = [3, 1, 3]
    periods["included_year_count"] = [3, 1, 1]
    periods["cohort_stock_coverage"] = [1.0, 1.0, .7]
    return {"final_efficiency": final, "periods": periods,
            "relationships": pd.DataFrame({"tech": final.tech,
                "mode": ["cars", "cars", "medium_trucks"], "powertrain": ["bev", "bev_300_miles", "bev"]})}


def test_final_csv_uses_recorded_consumption_before_service_conversion(bundle):
    inputs = prepared_period_inputs()
    periods = diagnostic._published_periods(bundle, inputs)
    service = periods.loc[periods.metric.eq("service_efficiency")].set_index("tech")
    energy = periods.loc[periods.metric.eq("consumption")].set_index("tech")
    assert service.value.to_dict() == {"T_C_BEV_EX": 2, "T_C_BEV300_N": 3, "T_MDV_T_BEV_EX": .8}
    assert service.loc["T_C_BEV_EX", ["vintage", "year", "source_year_start", "source_year_end"]].tolist() == pytest.approx([2020, 2022.6, 2021, 2023])
    assert service.loc["T_C_BEV300_N", ["vintage", "year"]].tolist() == [2025, 2030]
    assert energy.loc["T_C_BEV_EX", "value"] == 2.35
    assert energy.loc["T_C_BEV300_N", "value"] == pytest.approx(2 / 3)
    assert (energy.value * service.value).tolist() == pytest.approx(energy.load_factor.tolist())
    assert service.loc["T_C_BEV_EX", "powertrain"] == "bev_existing_representative"
    assert set(periods.marker_source) == {"Final prepared period CSV"}
    assert "Cohort-stock-weighted" in service.loc["T_C_BEV_EX", "time_basis"]
    assert "interval-end" in service.loc["T_C_BEV300_N", "time_basis"]
    # Annual reciprocal efficiencies are deliberately unavailable: the period
    # evidence is the authoritative consumption immediately before final conversion.
    assert "annual" not in inputs


def test_final_csv_retains_partial_year_and_stock_coverage(bundle):
    periods = diagnostic._published_periods(bundle, prepared_period_inputs())
    truck = periods.loc[periods.tech.eq("T_MDV_T_BEV_EX") & periods.metric.eq("consumption")].iloc[0]
    assert truck.vintage == 2020
    assert truck.year == truck.source_year_start == truck.source_year_end == 2023
    assert truck.candidate_year_count == 3 and truck.included_year_count == 1
    assert truck.cohort_stock_coverage == .7
    assert truck.load_factor == 4 and truck.value == 5


def test_final_csv_labels_empty_period_analogue_from_annual_components(bundle):
    inputs = prepared_period_inputs()
    inputs["annual"] = inputs["final_efficiency"][["region", "tech", "vintage", "input_comm", "output_comm"]].assign(
        included_in_period=True, treatment="ATB_PEV_level_NRCan_shared_anchor_history",
        analogue="gasoline", historical_index_anchor="bev_300_miles")
    periods = diagnostic._published_periods(bundle, inputs)
    assert periods.observation_basis.str.startswith("Configured analogue").all()
    assert set(periods.analogue) == {"gasoline"}
    assert periods.source_treatments.str.contains("shared_anchor_history").all()
    own = diagnostic._observation_summary([{"treatment": "ATB_PEV_level_NRCan_shared_anchor_history",
        "analogue": "bev_300_miles", "historical_index_anchor": "bev_300_miles"}])
    assert own["observation_basis"].startswith("Available source")


@pytest.mark.parametrize("field,value", [
    ("native_intensity", 0), ("native_intensity", np.inf),
    ("load_factor", np.nan), ("weighted_source_year", np.inf),
])
def test_final_csv_rejects_invalid_period_evidence(bundle, field, value):
    inputs = prepared_period_inputs()
    inputs["periods"].loc[0, field] = value
    with pytest.raises(ValueError, match="period consumption/load evidence"):
        diagnostic._published_periods(bundle, inputs)


def test_final_csv_rejects_reciprocal_average_inconsistent_with_period_consumption(bundle):
    inputs = prepared_period_inputs()
    inputs["final_efficiency"].loc[0, "efficiency"] = 3
    with pytest.raises(ValueError, match="disagrees with period consumption"):
        diagnostic._published_periods(bundle, inputs)


def test_final_csv_requires_new_period_contract_and_exact_keys(bundle):
    inputs = prepared_period_inputs()
    bad = {**inputs, "periods": inputs["periods"].drop(columns="native_intensity")}
    with pytest.raises(ValueError, match="Rebuild notebook evidence"):
        diagnostic._published_periods(bundle, bad)
    inputs["periods"] = inputs["periods"].iloc[1:]
    with pytest.raises(ValueError, match="period consumption/load evidence"):
        diagnostic._published_periods(bundle, inputs)


def period_markers():
    context = {"vehicle_group": "Cars", "mode": "cars", "atb_class": diagnostic.COHORT,
               "powertrain": "bev_300_miles", "resolution": "Actual production aggregation",
               "stage": "Indexed bridge", "series": "Production method"}
    records = []
    for year, consumption, load in [(2013, 3, 1), (2018, 1.1, 2), (2023, 1.6, 2.4), (2030, .8, 2.4)]:
        for metric, value, units in [("consumption", consumption, "MJ/vkm"),
                                     ("service_efficiency", load / consumption, "bn passenger-km/PJ")]:
            records.append({**context, "year": year, "value": value, "metric": metric, "units": units,
                            "load_factor": load, "sample_kind": "base_year_anchor" if year == 2023 else "period"})
    return pd.DataFrame(records)


def test_pchip_uses_only_period_markers_preserves_knots_and_service_conversion(bundle):
    markers = period_markers()
    clean = diagnostic.interpolate_period_markers(bundle, markers)
    spikes = markers.loc[markers.metric.eq("consumption")].iloc[:3].copy()
    spikes["year"], spikes["value"] = [2000, 2015, 2040], [1000, .0001, 2000]
    spikes["stage"], spikes["series"], spikes["sample_kind"] = "Annual calculation", "Annual production calculation", np.nan
    original = pd.concat([markers, spikes], ignore_index=True)
    fitted = diagnostic.interpolate_period_markers(bundle, original)
    curves = fitted.loc[fitted.sample_kind.eq("curve")]
    assert set(curves.stage) == {"Diagnostic interpolation"}
    pd.testing.assert_frame_equal(curves.reset_index(drop=True), clean.loc[clean.sample_kind.eq("curve")].reset_index(drop=True), check_dtype=False)
    pd.testing.assert_frame_equal(fitted.iloc[:len(original)][original.columns].reset_index(drop=True), original, check_dtype=False)
    assert curves.year.min() == 2013 and curves.year.max() == 2030
    for metric in ["consumption", "service_efficiency"]:
        values = curves.loc[curves.metric.eq(metric)].set_index("year").value
        knots = markers.loc[markers.metric.eq(metric)].set_index("year").value
        assert values.loc[knots.index].tolist() == pytest.approx(knots.tolist(), rel=1e-12)
    energy = curves.loc[curves.metric.eq("consumption")].set_index("year")
    service = curves.loc[curves.metric.eq("service_efficiency")].set_index("year")
    assert (energy.value * service.value).tolist() == pytest.approx(energy.load_factor.tolist())
    knots = markers.loc[markers.metric.eq("consumption")].sort_values("year")
    for left, right in zip(knots.itertuples(), knots.iloc[1:].itertuples(), strict=False):
        interval = energy.loc[energy.index.to_series().between(left.year, right.year), "value"]
        assert interval.min() >= min(left.value, right.value) - 1e-12
        assert interval.max() <= max(left.value, right.value) + 1e-12


def test_pchip_leaves_one_supported_year_as_markers_only(bundle):
    markers = period_markers().loc[lambda frame: frame.year.eq(2023)]
    result = diagnostic.interpolate_period_markers(bundle, markers)
    assert not result.sample_kind.eq("curve").any()
    pd.testing.assert_frame_equal(result.reset_index(drop=True), markers.reset_index(drop=True))


def test_pchip_ignores_markers_beyond_the_comparison_horizon(bundle):
    markers = period_markers()
    expected = diagnostic.interpolate_period_markers(bundle, markers)
    later = markers.loc[markers.year.eq(2030)].assign(year=2035, value=100)
    result = diagnostic.interpolate_period_markers(bundle, pd.concat([markers, later], ignore_index=True))
    pd.testing.assert_frame_equal(result.loc[result.sample_kind.eq("curve")].reset_index(drop=True),
        expected.loc[expected.sample_kind.eq("curve")].reset_index(drop=True))
    assert result.loc[result.year.eq(2035)].sample_kind.eq("period").all()


@pytest.mark.parametrize("field", ["value", "load_factor"])
def test_pchip_rejects_conflicting_knots(bundle, field):
    markers = period_markers()
    conflicting = markers.loc[markers.metric.eq("consumption") & markers.year.eq(2023)].copy()
    conflicting[field] *= 2
    with pytest.raises(ValueError, match="Conflicting period and base-year coordinates"):
        diagnostic.interpolate_period_markers(bundle, pd.concat([markers, conflicting], ignore_index=True))


@pytest.mark.parametrize("field,value", [("value", 0), ("value", np.inf), ("load_factor", -1), ("load_factor", np.nan)])
def test_pchip_rejects_invalid_consumption_and_load_knots(bundle, field, value):
    markers = period_markers()
    markers.loc[markers.metric.eq("consumption") & markers.year.eq(2018), field] = value
    with pytest.raises(ValueError, match="interpolation knots|Conflicting period and base-year coordinates"):
        diagnostic.interpolate_period_markers(bundle, markers)


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
            "benchmark_matches", "benchmark_allocations", "benchmark_weighted", "omega_records", "omega_buckets",
            "periods", "rating_class_weights", "annual_cohort_stock"} <= set(paths)
    outputs = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["outputs"]
    assert {"raw_ratings_" + component for component in outputs} <= set(paths)


def test_future_truck_powertrains_do_not_require_unobserved_historical_labels(bundle, monkeypatch):
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Heavy trucks": "heavy_trucks"})
    base = bundle.scenario.periods.base_year
    class Evidence:
        def derive_consumption(self, region, mode, powertrain, year, *, allow_analogue=False):
            if year <= base and powertrain != "diesel":
                pytest.fail("Unobserved electric history was invented")
            consumption = 2 if powertrain == "diesel" else .5
            return {"native_intensity": consumption, "native_unit": "MJ/vkm",
                    "treatment": "source projection", "analogue": powertrain,
                    "consumption_index": 1, "load_factor": 1}
        def period_observations(self, region, mode, powertrain, years, *, allow_analogue_when_empty=False):
            return {y: self.derive_consumption(region, mode, powertrain, y) for y in years}
        def atb_value(self, region, mode, powertrain, year):
            return 2 if powertrain == "diesel" else .5
    monkeypatch.setattr(diagnostic, "_evidence", lambda *args: Evidence())
    monkeypatch.setattr(diagnostic, "harmonised_ceud_history", lambda *args: pd.DataFrame())
    monkeypatch.setattr(diagnostic, "_cohort_counts", lambda config, inputs, mode, powertrain, vintage:
                        pd.Series(dtype=float) if powertrain == "bev" else
                        pd.Series(1.0, index=bundle.scenario.periods.historical_years(vintage)))
    atb = pd.DataFrame([
        {"region": "ON", "mode": "heavy_trucks", "vehicle_class": "Sleeper",
         "powertrain": powertrain, "year": year, "weighted_consumption": value,
         "consumption_mj_per_vkm": value}
        for year in [base, base + 7] for powertrain, value in [("diesel", 2), ("bev", .5)]
    ])
    loads = pd.DataFrame([{"region": "ON", "mode": "heavy_trucks", "year": base, "load_factor": 1}])
    support = bridge_support(bundle, "heavy_trucks", ["diesel", "bev"])
    support = support.loc[~(support.powertrain.eq("bev") & support.source_year.le(base))]
    bridge, _ = diagnostic._bridge_rows(bundle, {"atb": atb, "rating_weights": audit(), "ceud": pd.DataFrame({"year": [2000]}), "loads": loads, "annual": support})
    indexed = bridge.loc[bridge.stage.eq("Indexed bridge") & bridge.powertrain.eq("bev")]
    assert not indexed.empty and indexed.year.min() == base
    assert indexed.loc[indexed.year.eq(base), "sample_kind"].eq("base_year_anchor").all()
    assert indexed.loc[indexed.sample_kind.eq("period"), "year"].gt(base).all()
    annual = bridge.loc[bridge.stage.eq("Annual calculation") & bridge.powertrain.eq("bev")]
    assert not annual.empty and annual.year.min() == base + 7
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
    catalogue = audit().assign(aggregation_method="distinct_model_class_weighted", cohort_matched_stock=0)
    weighted = catalogue.assign(aggregation_method="distinct_model_fallback_no_resolved_stock")
    weighted.loc[weighted.year.eq(2023), "aggregation_method"] = "mto_stock_weighted"
    weighted.loc[weighted.year.eq(2023), "cohort_matched_stock"] = 10
    comparison = diagnostic._benchmark(bundle, {"rating_weights": catalogue, "benchmark_weighted": weighted})
    measured = comparison.loc[comparison.series.str.startswith("Diagnostic")]
    assert measured.loc[measured.year.eq(2023), "cohort_matched_stock"].eq(10).all()
    assert measured.loc[measured.year.eq(2010), "aggregation_method"].eq("distinct_model_fallback_no_resolved_stock").all()
    assert measured.loc[measured.year.eq(2010), "cohort_matched_stock"].eq(0).all()


def test_cohort_weights_use_raw_age_counts_before_eligibility_or_redistribution(bundle):
    inputs = {
        "relationships": pd.DataFrame([{"mode": "cars", "powertrain": "bev", "tech": "T_C_BEV_EX"}]),
        "final_efficiency": pd.DataFrame([{"region": "ON", "tech": "T_C_BEV_EX", "vintage": 2020}]),
        "annual_cohort_stock": pd.DataFrame([
            {"region": "ON", "tech": "T_C_BEV_EX", "vintage": 2020, "vintage_year": year,
             "raw_capacity": raw, "capacity": redistributed}
            for year, raw, redistributed in [(2021, 1, 0), (2022, 3, 40), (2023, 6, 60)]
        ]),
    }
    counts = diagnostic._cohort_counts(bundle, inputs, "cars", "bev_300_miles", 2020)
    assert counts.to_dict() == {2021: 1, 2022: 3, 2023: 6}


def test_statistic_selection_reuses_production_aggregation_without_changing_recorded_identity(bundle, monkeypatch):
    classified = ratings()
    aggregation = load_harmonization_rules(bundle, "road_aggregation")
    weights = pd.DataFrame([
        {"report_year": 2025, "weight_basis": aggregation["ldv_weight_basis"], "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": size, "aggregation_weight": weight}
        for size, weight in [("Compact", .75), ("Mid-size", .25)]
    ])
    summary = {"rating_statistic": "mean", "recorded_rating_statistic": "mean", "build": {"build_id": "recorded"},
               "inputs": {"ratings": {"sha256": "recorded-hash"}}}
    original = diagnostic.LDVEfficiencyDiagnostic(bundle, {}, summary, {"ratings": classified, "rating_class_weights": weights})
    seen = []
    def frames(config, inputs, *, recorded_statistic):
        seen.append((config.scenario.efficiencies.rating_statistic, recorded_statistic))
        return {"bridge": inputs["rating_weights"]}, {
            "rating_statistic": config.scenario.efficiencies.rating_statistic,
            "recorded_rating_statistic": recorded_statistic,
        }
    monkeypatch.setattr(diagnostic, "build_diagnostic_frames", frames)
    monkeypatch.setattr(diagnostic, "write_dataframe_atomic", lambda *args: pytest.fail("selector published a parameter"))
    assert diagnostic.select_rating_statistic(original, "mean") is original
    selected = diagnostic.select_rating_statistic(original, "median")
    assert seen == [("median", "mean")]
    assert selected.summary["build"] == original.summary["build"]
    assert selected.summary["inputs"] == original.summary["inputs"]
    actual = selected.frames["bridge"].loc[selected.frames["bridge"].year.eq(2023)].iloc[0]
    assert actual.native_class_mean == 40
    assert selected.bundle.scenario.efficiencies.rating_statistic == "median"
    assert original.bundle.scenario.efficiencies.rating_statistic == "mean"
    assert "rating_weights" not in original.inputs
    with pytest.raises(ValueError, match="Unsupported rating statistic"):
        diagnostic.select_rating_statistic(original, "mode")
