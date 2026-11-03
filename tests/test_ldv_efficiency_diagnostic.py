"""Source distributions, class resolution and recorded efficiency bridge contracts."""

from dataclasses import replace
from pathlib import Path
import runpy
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from parameterization.road_efficiencies import ProjectionAnchorUnavailable, select_atb_consumption
from utils import load_config_bundle, load_conversion_factors, load_harmonization_rules
from validation import ldv_efficiency_diagnostic as diagnostic


@pytest.fixture
def bundle():
    loaded = load_config_bundle("config/scenarios/default.yaml")
    geography = loaded.scenario.geography.model_copy(update={"regions": ["ON"]})
    return replace(loaded, scenario=loaded.scenario.model_copy(update={"geography": geography}))


def test_composition_view_reads_only_current_presentation_choices(bundle, tmp_path):
    import yaml

    paths = bundle.paths.model_dump(mode="json")
    paths["config"]["parameters"] = "live_view_parameters"
    (tmp_path / "config").mkdir()
    (tmp_path / "config/paths.yaml").write_text(yaml.safe_dump(paths), encoding="utf-8")
    directory = tmp_path / "live_view_parameters"
    directory.mkdir()
    rules = {"composition_years": list(range(2015, 2026)), "composition_top_size_classes": 7,
             "minimum_model_year": 2005, "distribution_year_end": 2025, "distribution_year_step": 2,
             "size_class_order": {"Cars": ["Compact"], "Light trucks": ["Minivan"]},
             "grid_plot_width": 1200, "line_plot_width": 900, "heatmap_plot_width": 760,
             "native_year_start": 2000, "bridge_years": [2010, 2040],
             "region": "IGNORE_LIVE_MODEL_CHOICE"}
    (directory / "rules.yaml").write_text(yaml.safe_dump({"parameterization": {
        "ldv_efficiency_diagnostic": rules}}), encoding="utf-8")
    captured = replace(bundle, repo_root=tmp_path)
    assert diagnostic.composition_view_settings(captured) == {"years": list(range(2015, 2026)), "top_size_classes": 7,
        "distribution_years": list(range(2005, 2026, 2)), "class_order": rules["size_class_order"],
        "grid_plot_width": 1200, "line_plot_width": 900, "heatmap_plot_width": 760,
        "native_year_start": 2000, "bridge_years": [2010, 2040]}
    assert captured.paths == bundle.paths
    assert captured.scenario == bundle.scenario


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


def test_phev_distribution_keeps_native_cd_separate_from_total_energy(bundle):
    source = ratings().iloc[[0]].assign(powertrain="phev_35_miles", native_unit="Le/100 km (CD)", consumption_mj_per_vkm=1.8)
    rows, summary = diagnostic.rating_distributions(bundle, source)
    assert rows.consumption_mj_per_vkm.tolist() == [1.8]
    assert summary.consumption_mj_per_vkm.tolist() == [1.8]
    assert summary.equal_row_mean.iloc[0] == 10
    assert summary.native_unit.iloc[0] == "Le/100 km (CD)"


def phev_ratings_and_weights(bundle):
    rows, weights = [], []
    conversions = load_conversion_factors(bundle)
    gasoline_hhv = conversions["fuel_hhv"]["gasoline_mj_per_litre"]
    for year, multiplier in [(2020, 1.5), (2022, 1.2), (2023, 1), (2026, .5)]:
        for size, weight, cs, cd_values in [("Compact", .25, 4, [1, 3]), ("Subcompact", .75, 8, [6])]:
            cs_value = cs * (2 - multiplier)
            annual_class = []
            for cd in cd_values:
                electricity, liquid_fuel = cd * multiplier * 5, cd * multiplier / 10
                cd_energy = (electricity * conversions["energy"]["kwh_to_mj"] + liquid_fuel * gasoline_hhv) / 100
                # Synthetic annual UF is fixed; neither component panel applies it.
                total_energy = .6 * cd_energy + .4 * cs_value * gasoline_hhv / 100
                annual_class.append(total_energy)
                rows.append({"included": True, "year": year, "rating_class": size, "powertrain": "phev_35_miles",
                    "Make": "Maker", "Model": size, "native_consumption": cd * multiplier,
                    "native_unit": "Le/100 km (CD)", "consumption_mj_per_vkm": total_energy,
                    "Fuel type 2": "X", "cd_electricity_kwh_per_100km": electricity,
                    "cd_fuel_l_per_100km": liquid_fuel,
                    "combined_cs_consumption_L_100_km": cs_value,
                    "combined_cd_consumption_Le_100_km": cd * multiplier})
            weights.append({"year": year, "powertrain": "phev_35_miles", "rating_class": size,
                "ceud_class": "Car", "weight": weight, "native_class_mean": np.mean(cd_values) * multiplier,
                "class_mean_mj_per_vkm": np.mean(annual_class)})
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
    rows, weights = phev_ratings_and_weights(bundle)
    original = rows.copy(deep=True)
    result = diagnostic.phev_rating_components(bundle, rows, weights)
    selected = result.loc[result.year.eq(2023)].set_index("component")
    conversions = load_conversion_factors(bundle)
    assert selected.loc["CS", "native_value"] == pytest.approx(7)
    assert selected.loc["CD", "native_value"] == pytest.approx(5)
    assert selected.loc["CS", "value"] == pytest.approx(7 * conversions["fuel_hhv"]["gasoline_mj_per_litre"] / 100)
    assert selected.loc["CD", "value"] == pytest.approx((25 * conversions["energy"]["kwh_to_mj"] + .5 * conversions["fuel_hhv"]["gasoline_mj_per_litre"]) / 100)
    assert set(result.component) == {"CS", "CD"} and not result.utility_weighted.any()
    assert "explicit liquid fuel" in selected.loc["CD", "native_basis"]
    assert selected.loc["CD", "energy_basis"] != selected.loc["CS", "energy_basis"]
    pd.testing.assert_frame_equal(rows, original)
    assert rows.consumption_mj_per_vkm.notna().all()


def test_phev_components_use_catalogue_median_or_selected_empirical_weights(bundle):
    source, original_weights = phev_ratings_and_weights(bundle)
    source = source.loc[source.included & source.year.eq(2023)].copy()
    source["rating_class"] = "Compact"
    source["Model"] = ["A", "B", "C"]
    source["combined_cs_consumption_L_100_km"] = [2, 8, 30]
    source["combined_cd_consumption_Le_100_km"] = [1, 5, 20]
    source["cd_electricity_kwh_per_100km"] = [5, 25, 100]
    source["cd_fuel_l_per_100km"] = [.1, .5, 2]
    source["native_consumption"] = source.combined_cd_consumption_Le_100_km
    source["component"], source["source_row"] = "phev", [1, 2, 3]
    weights = original_weights.loc[original_weights.year.eq(2023) & original_weights.rating_class.eq("Compact")].assign(weight=1)
    fallback = diagnostic.phev_rating_components(bundle, source, weights).set_index("component")
    assert fallback.loc["CD", "native_value"] == 5
    assert fallback.loc["CS", "native_value"] == 8
    allocations = source[["year", "rating_class", "powertrain", "component", "source_row"]].assign(
        region="ON", variant_weight=[.8, .1, .1])
    empirical = diagnostic.phev_rating_components(bundle, source, weights, allocations).set_index("component")
    assert empirical.loc["CD", "native_value"] == pytest.approx(3.3)
    assert empirical.loc["CS", "native_value"] == pytest.approx(5.4)


def test_phev_component_comparison_rejects_missing_cd_class_and_invalid_values(bundle):
    rows, weights = phev_ratings_and_weights(bundle)
    with pytest.raises(ValueError, match="missing a recorded rating class"):
        diagnostic.phev_rating_components(bundle, rows.loc[rows.rating_class.eq("Compact")], weights)
    for field in ("combined_cs_consumption_L_100_km", "combined_cd_consumption_Le_100_km",
                  "cd_electricity_kwh_per_100km", "cd_fuel_l_per_100km"):
        for invalid in [np.nan, np.inf, -1]:
            bad = rows.copy()
            bad.loc[0, field] = invalid
            with pytest.raises(ValueError, match="finite non-negative"):
                diagnostic.phev_rating_components(bundle, bad, weights)


def test_phev_components_use_each_records_liquid_fuel_hhv(bundle):
    rows, weights = phev_ratings_and_weights(bundle)
    rows.loc[rows.rating_class.eq("Subcompact"), "Fuel type 2"] = "D"
    result = diagnostic.phev_rating_components(bundle, rows, weights)
    selected = result.loc[result.year.eq(2023)].set_index("component")
    factors = load_conversion_factors(bundle)["fuel_hhv"]
    gasoline, diesel = factors["gasoline_mj_per_litre"], factors["diesel_mj_per_litre"]
    assert selected.loc["CS", "value"] == pytest.approx((.25 * 4 * gasoline + .75 * 8 * diesel) / 100)
    assert selected.loc["CD", "value"] == pytest.approx((25 * 3.6 + .25 * .2 * gasoline + .75 * .6 * diesel) / 100)
    rows.loc[0, "Fuel type 2"] = "unsupported"
    with pytest.raises(ValueError, match="supported liquid fuel"):
        diagnostic.phev_rating_components(bundle, rows, weights)


def test_native_phev_shows_independent_cd_without_cs_curves(bundle, monkeypatch):
    rows, _ = phev_ratings_and_weights(bundle)
    distribution, _ = diagnostic.rating_distributions(bundle, rows)
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {})
    monkeypatch.setattr(diagnostic, "select_atb_consumption", lambda *args, **kwargs: pd.DataFrame())
    inputs = {"atb_phev": pd.DataFrame(columns=["trajectory"]), "atb_vehicles": pd.DataFrame(),
              "atb": pd.DataFrame(columns=["mode", "vehicle_class"])}
    native = diagnostic._raw_sources(bundle, inputs, distribution)
    selected = native.loc[native.year.eq(2023) & native.source_class.eq("Compact")].set_index("series")
    assert selected.loc["NRCan CD family-median fallback", "value"] == 2
    assert set(selected.units) == {"Le/100 km (CD)"}
    assert not native.series.str.contains("CS").any()


def test_native_phev_trajectory_uses_native_median_when_total_energy_has_different_support(bundle, monkeypatch):
    source = pd.DataFrame([
        {"included": True, "year": 2023, "rating_class": "Compact", "powertrain": "phev_35_miles",
         "Make": "Maker", "Model": model, "native_consumption": native, "native_unit": "Le/100 km (CD)",
         "consumption_mj_per_vkm": total, "component": "phev", "source_row": row,
         "combined_cd_consumption_Le_100_km": native}
        for row, model, native, total in [(1, "A", 10, 3), (2, "A AWD", 20, 1), (3, "A 4WD", 30, 2)]
    ])
    distribution, _ = diagnostic.rating_distributions(bundle, source)
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {})
    monkeypatch.setattr(diagnostic, "select_atb_consumption", lambda *args, **kwargs: pd.DataFrame())
    inputs = {"atb_phev": pd.DataFrame(columns=["trajectory"]), "atb_vehicles": pd.DataFrame(),
              "atb": pd.DataFrame(columns=["mode", "vehicle_class"])}
    native = diagnostic._raw_sources(bundle, inputs, distribution)
    assert native.value.item() == 20
    assert native.units.item() == "Le/100 km (CD)"


def phev_bridge_inputs(bundle):
    rows, weights = phev_ratings_and_weights(bundle)
    return {
        "ratings": rows, "rating_weights": weights, "ceud": pd.DataFrame({"year": [2000]}),
        "atb": pd.DataFrame([
            {"region": "ON", "mode": "cars", "vehicle_class": "Compact", "year": year,
             "powertrain": "phev_35_miles", "consumption_mj_per_vkm": value, "weighted_consumption": value}
            for year, value in [(2020, 10), (2023, 8), (2025, 4), (2030, 3)]
        ]),
        "loads": pd.DataFrame([
            {"region": "ON", "mode": "cars", "year": year, "load_factor": 3 if year == 2023 else 2}
            for year in range(2000, 2024)
        ]),
        "omega_buckets": pd.DataFrame([
            {"market_class": "car", "powertrain": family.upper(), "bucket": bucket, "share_of_all_sales": 1}
            for family, bucket in [("phev", "phev_35_miles"), ("bev", "bev_300_miles")]
        ]),
        "relationships": pd.DataFrame([{"mode": "cars", "powertrain": "phev", "tech": "T_C_PHEV_EX"}]),
        "annual_cohort_stock": pd.DataFrame([
            {"region": "ON", "road_class": "cars", "tech": "T_C_PHEV_EX", "vintage": 2020,
             "vintage_year": year, "raw_capacity": count}
            for year, count in [(2021, 1), (2022, 3), (2023, 6)]
        ]),
        "final_efficiency": pd.DataFrame([{"region": "ON", "tech": "T_C_PHEV_EX", "vintage": 2020}]),
    }


def test_bridge_keeps_observed_phev_energy_and_projects_from_weighted_cohort(bundle, monkeypatch):
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Cars": "cars"})
    inputs = phev_bridge_inputs(bundle)
    bridge, anchors = diagnostic._bridge_rows(bundle, inputs)
    harmonised = bridge.loc[bridge.stage.eq("Harmonised before indexing")]
    assert set(harmonised.series) == {"NRCan size-class aggregate",
                                     "NRCan CD component-energy aggregate", "ATB absolute total energy"}
    assert not bridge.series.str.contains("backcast|Exploratory", case=False).any()
    assert not bridge.sample_kind.eq("curve").any()
    observed = diagnostic._rating_series(inputs["rating_weights"]).set_index("year").value
    annual = bridge.loc[bridge.stage.eq("Annual calculation") & bridge.atb_class.eq(diagnostic.COHORT)].set_index("year")
    assert annual.loc[2020, "value"] == pytest.approx(observed[2020])
    assert annual.loc[2023, "value"] == pytest.approx(observed[2023])
    assert annual.loc[2020, "treatment"] == "NRCan_observed_class_weighted"
    assert 2021 not in annual.index and 2026 not in annual.index
    weighted = (3 * observed[2022] + 6 * observed[2023]) / 9
    assert annual.loc[2025, "value"] == pytest.approx(weighted)
    assert annual.loc[2030, "value"] == pytest.approx(weighted * 3 / 4)
    assert annual.loc[2030, "load_factor"] == pytest.approx(8 / 3)
    assert annual.loc[2030, "treatment"] == "NRCan_cohort_anchor_ATB_relative_projection"
    production = bridge.loc[bridge.series.eq("Production method") & bridge.metric.eq("consumption")
                            & bridge.powertrain.eq("phev_35_miles") & bridge.atb_class.eq(diagnostic.COHORT)]
    historical = production.loc[production.sample_kind.eq("period")].iloc[0]
    assert historical.vintage == 2020 and historical.year == 2023
    assert historical.weighted_source_year == pytest.approx((3 * 2022 + 6 * 2023) / 9)
    assert historical.source_years == "2022 | 2023" and historical.cohort_stock_coverage == .9
    anchor = production.loc[production.sample_kind.eq("base_year_anchor")].iloc[0]
    assert anchor.year == 2025 and anchor.value == pytest.approx(weighted)
    assert anchor.source_year_start == 2022 and anchor.source_year_end == 2023
    assert set(anchors.anchor_powertrain) == {"phev_35_miles"}
    assert set(anchors.atb_reference_year) == {2025}
    assert anchors.parameter_reference_consumption_mj_per_vkm.tolist() == pytest.approx([weighted, weighted])


@pytest.mark.parametrize("class_only", [True, False])
def test_bridge_reports_only_diagnostic_class_anchor_absence(bundle, monkeypatch, class_only):
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Cars": "cars"})
    original = diagnostic._evidence
    def missing_anchor(*args):
        raise ProjectionAnchorUnavailable("No observed or approved-proxy LDV projection anchor: synthetic")
    def evidence(config, inputs, mode, atb_class):
        result = original(config, inputs, mode, atb_class)
        if atb_class is not None or not class_only:
            result.projection_anchor = missing_anchor
        return result
    monkeypatch.setattr(diagnostic, "_evidence", evidence)
    if not class_only:
        with pytest.raises(ProjectionAnchorUnavailable):
            diagnostic._bridge_rows(bundle, phev_bridge_inputs(bundle))
        return
    bridge, anchors = diagnostic._bridge_rows(bundle, phev_bridge_inputs(bundle))
    missing = bridge.loc[bridge.stage.eq("Missing class coverage")]
    assert missing.atb_class.tolist() == ["Compact"]
    assert missing.value.isna().all() and missing.year.eq(2025).all()
    assert set(anchors.atb_class) == {diagnostic.COHORT}
    assert not ((bridge.atb_class.eq("Compact")) & bridge.year.gt(2023) & bridge.value.notna()
                & bridge.stage.isin(["Annual calculation", "Indexed bridge"])).any()


def test_bridge_does_not_hide_other_class_projection_failures(bundle, monkeypatch):
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {"Cars": "cars"})
    original = diagnostic._evidence
    def invalid_stock(*args):
        raise ValueError("Invalid LDV projection cohort stock: synthetic")
    def evidence(config, inputs, mode, atb_class):
        result = original(config, inputs, mode, atb_class)
        if atb_class is not None:
            result.projection_anchor = invalid_stock
        return result
    monkeypatch.setattr(diagnostic, "_evidence", evidence)
    with pytest.raises(ValueError, match="Invalid LDV projection cohort stock"):
        diagnostic._bridge_rows(bundle, phev_bridge_inputs(bundle))


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
        for powertrain in ("gasoline", "bev_300_miles") for year, value in ((2023, 2), (2025, 1), (2030, .8))
    ])
    inputs = {"atb": atb, "rating_weights": audit(), "ceud": pd.DataFrame(),
              "loads": pd.DataFrame([
                  {"region": "ON", "mode": "cars", "year": year, "load_factor": 2}
                  for year in (2010, 2021, 2022, 2023)
              ]),
              "relationships": pd.DataFrame([
                  {"tech": tech, "powertrain": family}
                  for tech, family in [("T_C_BEV_EX", "bev"), ("T_C_GSL_EX", "gasoline")]
              ]),
              "annual_cohort_stock": pd.DataFrame([
                  {"region": "ON", "road_class": "cars", "tech": tech, "vintage_year": year, "raw_capacity": count}
                  for tech in ("T_C_BEV_EX", "T_C_GSL_EX") for year, count in [(2021, 1), (2022, 3), (2023, 6)]
              ])}
    evidence = diagnostic._evidence(bundle, inputs, "cars", "Compact")
    assert evidence.ratings.year.max() == 2023
    assert evidence.ldv_baseline("ON", "cars", "gasoline")[0] == pytest.approx(1.6)
    anchor = evidence.projection_anchor("ON", "cars", "bev_300_miles")
    assert anchor["included_years"] == [2023] and anchor["cohort_stock_coverage"] == .6
    assert anchor["stock_powertrain"] == "bev"
    assert evidence.derive_consumption("ON", "cars", "bev_300_miles", 2025)["native_intensity"] == pytest.approx(1.6)
    assert evidence.derive_consumption("ON", "cars", "bev_300_miles", 2030)["native_intensity"] == pytest.approx(1.6 * .8)


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
    assert service.loc["T_C_BEV_EX", ["vintage", "year", "source_year_start", "source_year_end"]].tolist() == pytest.approx([2020, 2023, 2021, 2023])
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
        included_in_period=True, treatment="NRCan_missing_year_approved_analogue_index", analogue="gasoline")
    periods = diagnostic._published_periods(bundle, inputs)
    assert periods.observation_basis.str.startswith("Configured analogue").all()
    assert set(periods.analogue) == {"gasoline"}
    assert periods.source_treatments.str.contains("approved_analogue_index").all()
    own = diagnostic._observation_summary([{"treatment": "NRCan_observed_class_weighted", "analogue": "bev_300_miles"}])
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


@pytest.mark.parametrize("suffix", [".csv", ".csv.gz"])
def test_loader_uses_recorded_snapshot_and_never_accepts_reader_scenario(bundle, tmp_path, monkeypatch, suffix):
    from validation import notebook_evidence

    source = tmp_path / ("actual" + suffix)
    pd.DataFrame({"value": [3]}).to_csv(source, index=False)
    metadata = {"build_id": "recorded-build", "built_at": "2026-10-17", "scenario": {"name": "recorded"}}
    recorded = SimpleNamespace(bundle=bundle, paths={"actual": source}, metadata=metadata)
    seen = []
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda kind, repo_root: seen.append((kind, repo_root)) or recorded)
    monkeypatch.setattr(diagnostic, "build_diagnostic_frames", lambda config, inputs, **view: ({"actual": inputs["actual"]}, {"base_year": config.scenario.periods.base_year}))
    monkeypatch.setattr(diagnostic, "write_dataframe_atomic", lambda *args: pytest.fail("loader wrote evidence"))
    loaded = diagnostic.load_diagnostic(repo_root=tmp_path)
    assert seen == [("efficiency", tmp_path)]
    assert loaded.frames["actual"].value.iloc[0] == 3
    assert loaded.summary["build"] == metadata
    assert loaded.summary["inputs"]["actual"]["path"] == str(source)
    with pytest.raises(TypeError):
        diagnostic.load_diagnostic("config/scenarios/default.yaml")


@pytest.mark.parametrize("allocation,statistic", [("equal_distinct_variants", "median"), ("family_median", "mean")])
def test_loader_rejects_old_trim_mean_capture_with_explicit_rebuild_instruction(bundle, monkeypatch, allocation, statistic):
    from validation import notebook_evidence

    recorded = SimpleNamespace(bundle=bundle, paths={}, metadata={})
    monkeypatch.setattr(notebook_evidence, "load_notebook_evidence", lambda *args, **kwargs: recorded)
    monkeypatch.setattr(notebook_evidence, "prepare_command", lambda root: "uv run python explicit_rebuild.py")
    monkeypatch.setattr(diagnostic, "load_harmonization_rules", lambda *args: {
        "ratings": {"composition": {"variant_allocation": allocation, "family_statistic": statistic}}})
    monkeypatch.setattr(diagnostic, "build_diagnostic_frames", lambda *args, **kwargs: pytest.fail("Old evidence was relabelled"))
    with pytest.raises(ValueError, match="predates.*family-median.*explicit_rebuild"):
        diagnostic.load_diagnostic()


def test_collector_captures_raw_final_and_empirical_match_evidence(bundle, tmp_path, monkeypatch):
    benchmarks = {"benchmark_" + key: tmp_path / (key + ".csv")
                  for key in ("statcan_new", "statcan_stock", "mto_passenger_cohorts", "mto_commercial_cohorts", "mto_mapped", "izev_records")}
    monkeypatch.setattr(diagnostic, "collect_composition_benchmark_inputs", lambda config: benchmarks if config is bundle else pytest.fail("wrong config"))
    paths = diagnostic.collect_efficiency_inputs(bundle)
    assert {"atb_vehicles", "atb_phev", "ceud", "final_efficiency", "final_splits", "loads", "relationships",
            "omega_records", "omega_buckets",
            "periods", "rating_class_weights", "annual_cohort_stock", "composition_observations",
            "composition_variants", "composition_matches", "composition_coverage", "catalogue_rating_weights"} <= set(paths)
    outputs = load_harmonization_rules(bundle, "nrcan_fuel_consumption_ratings")["outputs"]
    assert {"raw_ratings_" + component for component in outputs} <= set(paths)
    assert {key: paths[key] for key in benchmarks} == benchmarks


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
        def projection_anchor(self, region, mode, powertrain):
            return {"powertrain": powertrain, "included_years": [base], "candidate_years": [base],
                    "atb_reference_year": bundle.scenario.periods.model[0], "stock_powertrain": "diesel",
                    "stock_proxy": powertrain != "diesel", "cohort_stock_coverage": 1,
                    "native_intensity": 2 if powertrain == "diesel" else .5, "load_factor": 1,
                    "observations": {base: {"treatment": "CEUD fleet proxy", "analogue": "diesel"}}}
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
    assert not indexed.empty and indexed.year.min() == bundle.scenario.periods.model[0]
    assert indexed.loc[indexed.year.eq(bundle.scenario.periods.model[0]), "sample_kind"].eq("base_year_anchor").all()
    assert indexed.loc[indexed.sample_kind.eq("period"), "year"].gt(base).all()
    annual = bridge.loc[bridge.stage.eq("Annual calculation") & bridge.powertrain.eq("bev")]
    assert not annual.empty and set(annual.year) == {bundle.scenario.periods.model[0], base + 7}
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


def test_distribution_selector_preserves_native_engine_and_motor_measurements():
    frame = pd.DataFrame({"native_consumption": [6, 8, 12], "native_unit": ["L/100 km"] * 3,
        "Engine size (L)": [2, np.nan, 4], "Motor (kW)": [np.nan, 150, 0]})
    original = frame.copy(deep=True)
    engine, missing_engine = diagnostic.distribution_metric_rows(frame, "Engine size")
    assert engine.native_consumption.tolist() == [2, 4] and missing_engine == 1
    assert engine.native_unit.eq("Engine displacement (L)").all()
    motor, missing_motor = diagnostic.distribution_metric_rows(frame, "Motor power")
    assert motor.native_consumption.tolist() == [150] and missing_motor == 2
    assert motor.native_unit.eq("Electric motor power (kW)").all()
    consumption, missing = diagnostic.distribution_metric_rows(frame, "Consumption")
    assert missing == 0
    pd.testing.assert_frame_equal(consumption, original)
    pd.testing.assert_frame_equal(frame, original)
    absent, missing = diagnostic.distribution_metric_rows(frame.drop(columns="Motor (kW)"), "Motor power")
    assert absent.empty and missing == len(frame)
    with pytest.raises(ValueError, match="Unsupported NRCan distribution metric"):
        diagnostic.distribution_metric_rows(frame, "engine horsepower")


def test_phev_component_means_follow_empirical_source_row_weights_and_recipient_region(bundle):
    rows, weights = phev_ratings_and_weights(bundle)
    rows["component"] = "phev_ratings"
    rows["source_row"] = range(1, len(rows) + 1)
    compact = rows.loc[rows.included & rows.rating_class.eq("Compact")]
    selected = compact[["component", "source_row", "year", "powertrain", "rating_class"]].copy()
    # Unequal empirical family weights must replace the equal-model CD mean.
    selected["variant_weight"] = [.8, .2] * compact.year.nunique()
    selected["region"] = "ON"
    other = selected.assign(region="QC", variant_weight=[.2, .8] * compact.year.nunique())
    allocations = pd.concat([selected, other], ignore_index=True)
    baseline = diagnostic.phev_rating_components(bundle, rows, weights).query("year == 2023").set_index("component")
    actual = diagnostic.phev_rating_components(bundle, rows, weights, allocations).query("year == 2023").set_index("component")
    assert baseline.loc["CD", "native_value"] == 5
    assert actual.loc["CD", "native_value"] == pytest.approx(.25 * (.8 * 1 + .2 * 3) + .75 * 6)
    assert actual.loc["CS", "native_value"] == baseline.loc["CS", "native_value"]
    # Splitting one source row's weight across observations preserves its mass.
    divided = pd.concat([allocations.assign(variant_weight=allocations.variant_weight / 2)] * 2, ignore_index=True)
    duplicate = diagnostic.phev_rating_components(bundle, rows, weights, divided)
    pd.testing.assert_frame_equal(duplicate, diagnostic.phev_rating_components(bundle, rows, weights, allocations))


def test_empirical_component_decomposition_rejects_missing_rows_and_nonunit_mass(bundle):
    rows, weights = phev_ratings_and_weights(bundle)
    rows["component"], rows["source_row"] = "phev_ratings", range(1, len(rows) + 1)
    allocations = rows.loc[rows.included & rows.rating_class.eq("Subcompact"),
        ["component", "source_row", "year", "powertrain", "rating_class"]].assign(region="ON", variant_weight=.5)
    with pytest.raises(ValueError, match="sum to one"):
        diagnostic.phev_rating_components(bundle, rows, weights, allocations)
    allocations["variant_weight"] = 1
    allocations.loc[allocations.index[0], "source_row"] = 999999
    with pytest.raises(ValueError, match="missing NRCan source row"):
        diagnostic.phev_rating_components(bundle, rows, weights, allocations)


def test_class_recalculation_selects_one_recipient_region_before_normalizing(bundle):
    regional = pd.concat([audit().assign(region="ON"), audit().assign(region="QC", weight=10)], ignore_index=True)
    expected = diagnostic._class_rating_audit(bundle, audit(), "Compact")
    actual = diagnostic._class_rating_audit(bundle, regional, "Compact")
    pd.testing.assert_frame_equal(actual.drop(columns="region").reset_index(drop=True), expected.reset_index(drop=True))


def test_composition_comparison_preserves_empirical_methods_and_missing_catalogue_support(bundle):
    identity = {"region": "ON", "ceud_class": "Car", "year": 2023, "rating_class": "Compact",
                "powertrain": "bev_300_miles", "native_unit": "kWh/100 km"}
    production = pd.DataFrame([{**identity, "native_class_mean": 15, "class_mean_mj_per_vkm": .54,
        "aggregation_method": "izev_incentive_weighted", "empirical_families": 2},
        {**identity, "region": "QC", "native_class_mean": 30, "class_mean_mj_per_vkm": 1.08,
         "aggregation_method": "izev_incentive_weighted", "empirical_families": 2},
        {**identity, "year": 2024, "native_class_mean": 10, "class_mean_mj_per_vkm": .36,
         "aggregation_method": "catalogue_fallback_median_no_empirical_support", "empirical_families": 0}])
    previous = pd.DataFrame([{**identity, "native_class_mean": 20, "class_mean_mj_per_vkm": .72}])
    result = diagnostic.composition_comparison(bundle, {"rating_weights": production, "catalogue_rating_weights": previous})
    current = result.set_index("year")
    assert len(result) == 2
    assert current.loc[2023, "consumption_change_fraction"] == pytest.approx(-.25)
    assert current.loc[2023, "aggregation_method"] == "izev_incentive_weighted"
    assert pd.isna(current.loc[2024, "catalogue_mj_per_vkm"])


def test_old_notebook_capture_requests_an_explicit_rebuild(bundle):
    with pytest.raises(ValueError, match="Rebuild notebook evidence.*empirical NRCan composition"):
        diagnostic.build_diagnostic_frames(bundle, {"ratings": ratings()})


def test_composition_only_capture_requires_new_independent_benchmark_evidence(bundle):
    inputs = {key: pd.DataFrame() for key in ("composition_observations", "composition_variants", "composition_matches",
              "composition_coverage", "catalogue_rating_weights")}
    with pytest.raises(ValueError, match="Rebuild notebook evidence to capture composition benchmarks"):
        diagnostic.build_diagnostic_frames(bundle, inputs)


def test_composition_model_points_take_trim_medians_once_and_keep_source_ranges(bundle):
    source = ratings().assign(component="ratings", source_row=range(1, 6))
    points = diagnostic.composition_model_evidence(bundle, source)
    compact = points.loc[points.year.eq(2023)].set_index("Model")
    assert len(compact) == 3
    assert compact.loc["A", "model_native_consumption"] == 15
    assert compact.loc["A", "model_min_mj_per_vkm"] == pytest.approx(.36)
    assert compact.loc["A", "model_max_mj_per_vkm"] == pytest.approx(.72)
    assert compact.loc["A", "rating_row_ids"] == "ratings:1 | ratings:2"
    assert compact.catalogue_rows.sum() == 4
    assert set(points.vehicle_group) == {"Cars"}


def test_catalogue_family_median_and_native_trajectory_ignore_duplicate_trim_rows(bundle, monkeypatch):
    source = pd.DataFrame([
        {"included": True, "year": 2023, "rating_class": "Compact", "powertrain": "gasoline",
         "Make": "Maker", "Model": model, "native_consumption": value, "native_unit": "L/100 km",
         "consumption_mj_per_vkm": value * .4, "component": "ratings", "source_row": row,
         "combined_cd_consumption_Le_100_km": np.nan, "combined_cs_consumption_L_100_km": np.nan}
        for row, (model, value) in enumerate([("A", 10), ("A AWD", 20), ("A 4WD", 30), ("B", 100), ("A AWD", 20)], 1)
    ])
    original = source.copy(deep=True)
    points = diagnostic.composition_model_evidence(bundle, source).set_index("Model")
    assert set(points.index) == {"A", "B"}
    assert points.loc["A", "model_native_consumption"] == 20
    assert points.loc["A", "family_median_mj_per_vkm"] == 8
    assert points.loc["A", "distinct_trims"] == 3
    assert points.loc["A", "catalogue_rows"] == 4
    distribution, summary = diagnostic.rating_distributions(bundle, source)
    assert summary.equal_distinct_model_median.item() == 60
    monkeypatch.setattr(diagnostic, "VIEW_MODES", {})
    monkeypatch.setattr(diagnostic, "select_atb_consumption", lambda *args, **kwargs: pd.DataFrame())
    inputs = {"atb_phev": pd.DataFrame(columns=["trajectory"]), "atb_vehicles": pd.DataFrame(),
              "atb": pd.DataFrame(columns=["mode", "vehicle_class"])}
    native = diagnostic._raw_sources(bundle, inputs, distribution)
    assert native.series.item() == "NRCan family-median fallback"
    assert native.value.item() == 60
    pd.testing.assert_frame_equal(source, original)


def family_evidence_inputs():
    observations = pd.DataFrame([
        {"region": "ON", "empirical_observation_id": identifier, "province": province, "source_kind": "izev",
         "pool_geography": "national", "pool_provinces": "ON | QC", "range_assignment": "family_single_range"}
        for identifier, province in [("ON-A", "ON"), ("QC-A", "QC"), ("ON-B", "ON")]
    ])
    variants = pd.DataFrame([
        {"region": "ON", "empirical_observation_id": identifier, "year": 2023, "rating_class": "Compact",
         "powertrain": "bev_300_miles", "catalogue_powertrain": raw_powertrain,
         "empirical_family_make": "Maker", "empirical_family_model": family,
         "component": "bev", "source_row": row, "rating_variant_id": f"variant{row}",
         "native_consumption": value, "consumption_mj_per_vkm": value * .036, "native_unit": "kWh/100 km",
         "variant_weight": weight, "allocated_count": weight * 100}
        for identifier, family, row, raw_powertrain, value, weight in [
            ("ON-A", "A", 1, "bev_300_miles", 15, .3), ("ON-A", "A", 2, "bev_300_miles", 19, .3),
            ("QC-A", "A", 1, "bev_300_miles", 15, .1), ("QC-A", "A", 2, "bev_300_miles", 19, .1),
            ("ON-B", "B", 3, "bev_300_miles", 25, .2),
        ]
    ])
    audit = pd.DataFrame([{"region": "ON", "year": 2023, "rating_class": "Compact", "powertrain": "bev_300_miles",
        "native_class_mean": 18.6, "class_mean_mj_per_vkm": 18.6 * .036, "aggregation_method": "izev_incentive_weighted"}])
    return {"composition_observations": observations, "composition_variants": variants, "rating_weights": audit}


def test_family_dot_weights_combine_provinces_without_count_or_range_multiplication(bundle):
    inputs = family_evidence_inputs()
    original = inputs["composition_variants"].copy(deep=True)
    actual = diagnostic.composition_family_evidence(bundle, inputs).set_index("family_model")
    assert len(actual) == 2
    assert actual.loc["A", "family_count"] == 80
    assert actual.loc["A", "family_weight"] == pytest.approx(.8)
    assert actual.loc["A", "family_native_consumption"] == pytest.approx(17)
    assert actual.loc["A", "geography"] == "ON | QC"
    assert actual.loc["A", "variant_count"] == 2
    assert actual.loc["A", "catalogue_powertrain"] == "bev_300_miles"
    assert actual.loc["A", "powertrain"] == "bev_300_miles"
    assert actual.loc["A", "production_native_consumption"] == 18.6
    assert actual.family_weight.sum() == pytest.approx(1)
    assert (actual.family_median_mj_per_vkm * actual.family_weight).sum() == pytest.approx(18.6 * .036)
    pd.testing.assert_frame_equal(inputs["composition_variants"], original)


def test_family_native_median_uses_its_own_support_without_changing_total_energy(bundle):
    inputs = family_evidence_inputs()
    inputs["composition_observations"] = inputs["composition_observations"].iloc[[0]].copy()
    template = inputs["composition_variants"].iloc[0].to_dict()
    inputs["composition_variants"] = pd.DataFrame([
        {**template, "source_row": row, "rating_variant_id": str(row), "native_consumption": native,
         "consumption_mj_per_vkm": mj, "variant_weight": energy_weight,
         "native_median_weight": native_weight, "allocated_count": 100 * energy_weight}
        for row, native, mj, energy_weight, native_weight in [(1, 30, 1, 0., 0.), (2, 10, 2, 1., 0.), (3, 20, 3, 0., 1.)]
    ])
    actual = diagnostic.composition_family_evidence(bundle, inputs)
    assert actual.family_native_consumption.item() == 20
    assert actual.family_median_mj_per_vkm.item() == 2
    assert actual.family_count.item() == 100
    assert actual.family_weight.item() == 1
    assert actual.variant_count.item() == 3


def test_review_retains_ambiguous_families_and_clear_multitrim_assumptions_without_count_expansion():
    rows = pd.DataFrame([
        {"empirical_observation_id": identity, "source_kind": "izev", "year": 2023, "province": "ON",
         "source_count": count, "match_status": status, "matched_variant_count": variants,
         "candidate_families": alternatives, "selected": selected}
        for identity, count, status, variants, alternatives, selected in [
            ("ambiguous", 12, "ambiguous_model_family", 4, "Family A | Family B", False),
            ("median", 100, "resolved", 3, "Family C", True),
            ("single", 5, "resolved", 1, "Family D", True),
            ("policy", 9, "resolved", 2, "Family E", False),
            ("administrative", 50, "unsupported_transaction_type", 2, "Family F", False),
        ]
    ])
    original = rows.copy(deep=True)
    review = diagnostic.composition_review_cases(SimpleNamespace(frames={"composition_observations": rows})).set_index("empirical_observation_id")
    assert set(review.index) == {"ambiguous", "median", "policy"}
    assert review.source_count.sum() == 121
    assert review.loc["ambiguous", "candidate_families"] == "Family A | Family B"
    assert review.loc["median", "recommended_handling"].startswith("Leave as is")
    assert "no empirical weight" in review.loc["policy", "recommended_handling"]
    assert review.loc["ambiguous", "review_kind"] == "Unresolved identity / configuration"
    pd.testing.assert_frame_equal(rows, original)


def test_review_includes_positive_known_ldv_year_gaps_from_captured_mapping_only(bundle, tmp_path):
    import yaml

    directory = tmp_path / "captured_parameters"
    directory.mkdir()
    rules = load_harmonization_rules(bundle, "road_aggregation")
    (directory / "rules.yaml").write_text(yaml.safe_dump({"version": 2, "fetching": {},
        "parameterization": {"road_aggregation": rules}}), encoding="utf-8")
    mapping = pd.DataFrame([
        {"entry_type": "mto_crosswalk", "mto_make_code": make, "mto_model_code": model,
         "model_year_from": first, "model_year_to": last, "canonical_make": "Captured Honda",
         "canonical_model": "CR-V", "vehicle_scope": scope, "mapping_status": "reviewed",
         "nrcan_vehicle_class": "Sport utility vehicle: Small"}
        for make, model, first, last, scope in [
            ("HOND", "CRV", 1997, 2023, "ldv"), ("HOND", "CRV", 2025, 2025, "ldv"),
            ("HEAV", "TRK", 1997, 2023, "mhdv"),
            ("MIXD", "KEY", 1997, 2023, "ldv"), ("MIXD", "KEY", 2025, 2025, "mhdv"),
        ]
    ])
    mapping.to_csv(directory / rules["vehicle_size_class_map_file"], index=False)
    captured = replace(bundle, paths=bundle.paths.model_copy(update={
        "config": bundle.paths.config.model_copy(update={"parameters": str(directory)})}))
    mapped = pd.DataFrame([
        {"report_year": 2025, "MAKE": make, "MODEL": model, "MODEL_YEAR": year, "FIT_ACTIVE": count,
         "mapping_accepted": False, "mapping_outcome": outcome}
        for make, model, year, count, outcome in [
            ("HOND", "CRV", 2024, 12, "unmapped"),
            ("HOND", "CRV", 2023, 15, "unmapped"),  # An interval exists: not a year-gap review case.
            ("HOND", "CRV", 2026, 0, "unmapped"),
            ("HOND", "CRV", 1990, 10, "out_of_scope"),
            ("HEAV", "TRK", 2024, 40, "unmapped"),
            ("MIXD", "KEY", 2024, 50, "unmapped"),
            ("RAWW", "UNK", 2024, 60, "unmapped"),
        ]
    ])
    item = diagnostic.LDVEfficiencyDiagnostic(captured, {"composition_observations": pd.DataFrame()}, {},
        {"benchmark_mto_mapped": mapped})
    review = diagnostic.composition_review_cases(item)
    assert len(review) == 1
    row = review.iloc[0]
    assert row.source_count == 12 and row.year == 2024
    assert row.raw_make == "HOND" and row.raw_model == "CRV"
    assert row.reviewed_mapping_year_intervals == "1997–2023 | 2025–2025"
    assert row.candidate_families == "Captured Honda / CR-V"
    assert row.match_status == "missing_applicable_reviewed_mapping_year"
    assert not row.selected
    assert row.family_make == row.family_model == row.rating_class == row.resolved_powertrain == ""
    assert "no reviewed interval" in row.identity_exclusion_reason
    assert "do not extend" in row.recommended_handling


def test_detail_filters_retain_ambiguous_candidates_and_separate_unknown_context(bundle):
    observations = pd.DataFrame([
        {"empirical_observation_id": identifier, "source_kind": "mto", "year": 2023,
         "rating_class": size, "resolved_powertrain": resolved, "candidate_powertrains": candidates,
         "candidate_rating_classes": candidate_classes, "match_status": status}
        for identifier, size, resolved, candidates, candidate_classes, status in [
            ("shared", "Compact", "", "gasoline | hev", "Compact", "ambiguous_powertrain_or_range"),
            ("size_unknown", "", "", "gasoline", "Compact | Mid-size", "ambiguous_rating_class"),
            ("unknown", "", "", "", "", "unmatched_ratings"),
        ]
    ])
    matches = pd.DataFrame({"empirical_observation_id": ["shared", "shared", "size_unknown"],
                            "component": ["ratings"] * 3, "source_row": [1, 2, 3],
                            "variant_weight": [0, 0, 0]})
    empty_variants = pd.DataFrame(columns=["empirical_observation_id", "variant_weight"])
    families = pd.DataFrame(columns=["source_kind", "year", "rating_class", "powertrain"])
    coverage = pd.DataFrame([{"year": 2023, "rating_class": "Compact", "powertrain": "gasoline"}])
    audit = pd.DataFrame([{"year": 2023, "rating_class": "Compact", "powertrain": "gasoline",
                          "native_class_mean": 8, "class_mean_mj_per_vkm": 3.2}])
    item = diagnostic.LDVEfficiencyDiagnostic(bundle,
        {"composition_observations": observations, "composition_matches": matches, "composition_variants": empty_variants,
         "composition_families": families, "composition_coverage": coverage}, {}, {"rating_weights": audit, "catalogue_rating_weights": audit.copy()})
    actual = diagnostic.composition_detail_frames(item, "mto", year=2023, rating_class="Compact", powertrain="gasoline")
    assert set(actual["observations"].empirical_observation_id) == {"shared", "size_unknown"}
    assert set(actual["matches"].source_row) == {1, 2, 3}
    assert set(actual["unresolved"].empirical_observation_id) == {"shared", "size_unknown", "unknown"}
    assert actual["families"].empty


def test_catalogue_median_reference_omits_only_wholly_identical_trajectories():
    points = [
        {"vehicle_group": "Cars", "atb_class": diagnostic.COHORT, "powertrain": powertrain,
         "metric": "consumption", "stage": "Indexed bridge", "sample_kind": kind,
         "year": year, "vintage": vintage, "value": value}
        for powertrain in ("gasoline", "diesel", "phev_35_miles")
        for kind, year, vintage, value in [("period", 2020, 2015, 2.),
            ("base_year_anchor", 2025, np.nan, 1.9), ("period", 2030, 2025, 1.8)]
    ]
    production = pd.DataFrame(points)
    reference = production.copy(deep=True)
    reference.loc[reference.powertrain.eq("gasoline") & reference.year.eq(2030), "value"] = 2.1
    reference = pd.concat([reference, pd.DataFrame([{**points[-1], "year": 2010, "vintage": 2005}])], ignore_index=True)
    original = reference.copy(deep=True)
    actual = diagnostic.distinct_catalogue_reference(production, reference)
    assert set(actual.powertrain) == {"gasoline", "phev_35_miles"}
    assert len(actual) == 7
    # The source-year anchor and interval-end period must remain separate.
    gasoline = actual.loc[actual.powertrain.eq("gasoline")]
    assert gasoline.loc[gasoline.year.eq(2025), "sample_kind"].item() == "base_year_anchor"
    assert gasoline.loc[gasoline.year.eq(2030), "value"].item() == 2.1
    pd.testing.assert_frame_equal(reference, original)


def test_detail_class_table_retains_catalogue_median_and_original_outer_weight(bundle):
    observations = pd.DataFrame(columns=["empirical_observation_id", "source_kind", "year", "rating_class",
        "resolved_powertrain", "candidate_powertrains", "match_status"])
    coverage = pd.DataFrame([{"year": 2023, "rating_class": "Compact", "powertrain": "gasoline",
                             "aggregation_method": "catalogue_fallback_median_insufficient_identity_support"}])
    audit = pd.DataFrame([{"year": 2023, "rating_class": "Compact", "powertrain": "gasoline",
        "native_class_mean": 8., "class_mean_mj_per_vkm": 3.2, "weight": .4, "native_unit": "L/100 km"}])
    frames = {"composition_observations": observations,
        "composition_matches": pd.DataFrame(columns=["empirical_observation_id"]),
        "composition_variants": pd.DataFrame(columns=["empirical_observation_id", "variant_weight"]),
        "composition_families": pd.DataFrame(columns=["source_kind", "year", "rating_class", "powertrain"]),
        "composition_coverage": coverage}
    item = diagnostic.LDVEfficiencyDiagnostic(bundle, frames, {}, {"rating_weights": audit, "catalogue_rating_weights": audit.copy()})
    actual = diagnostic.composition_detail_frames(item, "mto", year=2023, rating_class="Compact", powertrain="gasoline")
    result = actual["coverage"].iloc[0]
    assert result.aggregation_weight == .4
    assert result.production_mj_per_vkm == result.catalogue_mj_per_vkm == 3.2
    assert result.production_native_consumption == result.catalogue_native_consumption == 8
    assert actual["families"].empty


def weighted_detail_diagnostic(bundle):
    inputs = family_evidence_inputs()
    inputs["composition_variants"] = inputs["composition_variants"].assign(year=2015, Make="Maker", Model="Shared catalogue model")
    observations = inputs["composition_observations"].assign(year=2015, rating_class="Compact",
        observed_powertrain="bev", resolved_powertrain="bev_300_miles", candidate_powertrains="bev_300_miles",
        candidate_rating_classes="Compact", match_status="resolved", selection_status="selected", selected=True,
        raw_make="Maker", raw_model=["Description A", "Description A", "Description B"],
        family_make="Maker", family_model=["A", "A", "B"], source_count=[60, 20, 20],
        relative_contribution=[.6, .2, .2], source_model_year_min=2020, source_model_year_max=2024,
        source_model_years="2020 | 2023", composition_basis="historical_pooled_model_year_window")
    inputs["composition_observations"] = observations
    inputs["rating_weights"] = inputs["rating_weights"].assign(year=2015, native_unit="kWh/100 km", weight=.4,
        aggregation_method="izev_historical_composition_proxy", composition_basis="historical_pooled_model_year_window",
        observations=3, distinct_models=1, empirical_families=2, selected_source_count=100,
        source_model_year_min=2020, source_model_year_max=2024, pool_geography="national")
    catalogue_value = 19  # One catalogue family: median of its three distinct trims.
    inputs["catalogue_rating_weights"] = inputs["rating_weights"].assign(native_class_mean=catalogue_value,
        class_mean_mj_per_vkm=catalogue_value * .036)
    frames = {"composition_observations": observations,
        "composition_variants": inputs["composition_variants"],
        "composition_matches": inputs["composition_variants"].assign(selected=True, assigned_powertrain="bev_300_miles"),
        "composition_families": diagnostic.composition_family_evidence(bundle, inputs),
        "composition_coverage": inputs["rating_weights"].copy()}
    return diagnostic.LDVEfficiencyDiagnostic(bundle, frames, {"region": "ON"}, inputs)


def test_detail_compares_actual_catalogue_baseline_and_distinct_weighted_models(bundle):
    item = weighted_detail_diagnostic(bundle)
    original = {key: frame.copy(deep=True) for key, frame in item.frames.items()}
    frames = diagnostic.composition_detail_frames(item, "izev", year=2015)
    row = frames["coverage"].iloc[0]
    assert row.production_native_consumption == pytest.approx(18.6)
    assert row.catalogue_native_consumption == pytest.approx(19)
    assert row.production_mj_per_vkm == pytest.approx(18.6 * .036)
    assert row.catalogue_mj_per_vkm == pytest.approx(19 * .036)
    assert row.catalogue_rows == 3 and row.catalogue_models == 1
    assert row.empirical_families == 2 and row.weighted_catalogue_models == 1
    assert row.aggregation_weight == .4
    assert set(frames["families"].source_model_years) == {"2020 | 2023"}
    for key, frame in original.items():
        pd.testing.assert_frame_equal(item.frames[key], frame)


def test_izev_detail_excludes_resolved_other_ranges_but_keeps_ambiguous_candidates(bundle):
    item = weighted_detail_diagnostic(bundle)
    original = item.frames["composition_observations"].iloc[[0]].copy()
    observations = pd.concat([original.assign(empirical_observation_id=identifier, observed_powertrain="phev",
        resolved_powertrain=resolved, candidate_powertrains=candidates, match_status=status, selected=status == "resolved")
        for identifier, resolved, candidates, status in [
            ("selected35", "phev_35_miles", "phev_35_miles", "resolved"),
            ("selected50", "phev_50_miles", "phev_50_miles", "resolved"),
            ("ambiguous", "", "phev_35_miles | phev_50_miles", "ambiguous_powertrain_or_range"),
            ("outside50", "phev_50_miles", "phev_50_miles", "outside_supported_program_window"),
            ("unknown", "", "", "unmatched_ratings"),
        ]], ignore_index=True)
    matches = pd.DataFrame({"empirical_observation_id": ["selected35", "selected50", "ambiguous", "ambiguous", "outside50"],
        "powertrain": ["phev_35_miles", "phev_50_miles", "phev_35_miles", "phev_50_miles", "phev_50_miles"],
        "selected": [True, True, False, False, False], "variant_weight": [1., 1., 0., 0., 0.]})
    frames = {**item.frames, "composition_observations": observations, "composition_matches": matches,
        "composition_variants": item.frames["composition_variants"].iloc[:0]}
    actual = diagnostic.composition_detail_frames(replace(item, frames=frames), "izev", powertrain="phev_35_miles")
    assert set(actual["observations"].empirical_observation_id) == {"selected35", "ambiguous", "unknown"}
    assert set(actual["matches"].empirical_observation_id) == {"selected35", "ambiguous"}
    assert set(actual["unresolved"].empirical_observation_id) == {"ambiguous", "unknown"}
    selected = actual["matches"].loc[actual["matches"].selected]
    assert selected.powertrain.eq("phev_35_miles").all() and selected.variant_weight.sum() == 1


def test_composition_detail_tables_put_paired_values_counts_and_units_before_audit(bundle):
    import marimo as mo

    item = weighted_detail_diagnostic(bundle)
    frames = diagnostic.composition_detail_frames(item, "izev", year=2015)
    empty = {name: frame.iloc[:0].copy() for name, frame in frames.items()}
    app = runpy.run_path(str(Path(__file__).resolve().parents[1] / "docs/insights/ldv_efficiency_diagnosis.py"))["app"]
    cell = next(data.cell for data in app._cell_manager.cell_data() if "detailed_evidence" in data.cell.defs)
    tables = []

    class Controls:
        value = {"year": 2015, "rating_class": None, "powertrain": None}

        def __getitem__(self, name):
            return mo.md(name)

    def table_ui(frame, label):
        tables.append((label, frame.copy(deep=True)))
        return mo.md(label)

    cell.run(composition_detail_frames=lambda _item, kind, **_filters: frames if kind == "izev" else empty,
        diagnostic=item, mo=mo, mto_filters=Controls(), pev_filters=Controls(), pd=pd, table_ui=table_ui)
    consumption = next(frame for label, frame in tables if label == "Recorded class estimates: consumption, catalogue size and contributing evidence")
    assert list(consumption) == ["Target model year", "Size class", "Powertrain / range", "Cohort fallback median", "Production", "Native unit",
        "Cohort fallback median (total MJ/vkm)", "Production (total MJ/vkm)", "Catalogue − production (MJ/vkm)", "Catalogue rows", "Distinct families",
        "Weighted catalogue families", "Source descriptions", "Contributing source count", "Method"]
    assert consumption["Cohort fallback median"].item() == pytest.approx(19)
    assert consumption["Production"].item() == pytest.approx(18.6)
    assert consumption["Target model year"].item() == 2015
    assert consumption["Catalogue − production (MJ/vkm)"].item() == pytest.approx(
        consumption["Cohort fallback median (total MJ/vkm)"].item() - consumption["Production (total MJ/vkm)"].item())
    assert consumption["Native unit"].item() == "kWh/100 km"
    assert consumption["Source descriptions"].item() == 2
    assert consumption["Weighted catalogue families"].item() == 1
    assert consumption["Method"].item() == "iZEV proxy mean"
    context = next(frame for label, frame in tables if label == "Target year versus composition window and geographic pool")
    assert context["Composition source MYs"].item() == "2020–2024"
    assert context["Geographic pool"].item() == "national"
    descriptions = next(frame for label, frame in tables if label == "Contributing source descriptions and their actual counts")
    assert descriptions["Source count"].sum() == 100
    assert descriptions["Selected-count weight (%)"].tolist() == pytest.approx([80, 20])
    assert "empirical_observation_ids" not in descriptions
    technical = next(frame for label, frame in tables if label == "All class fields · original names")
    assert "class_mean_mj_per_vkm" in technical and "native_class_mean" in technical


@pytest.mark.parametrize("vehicle_group", ["Cars", "Light trucks"])
def test_composition_plot_emits_small_html_without_losing_points_or_audit_tables(vehicle_group):
    import altair as alt
    import marimo as mo

    models, families, summaries = [], [], []
    audit = "not-for-chart|" * 20_000
    for group, size in [("Cars", "Compact"), ("Light trucks", "Sport utility vehicle: Small")]:
        cohort = {"year": 2022, "rating_class": size, "powertrain": "gasoline", "native_unit": "L/100 km"}
        models.extend({**cohort, "vehicle_group": group, "Model": name, "make_model": name,
                       "family_median_mj_per_vkm": value, "model_native_consumption": value * 3,
                       "rating_row_ids": audit}
                      for name, value in [("Model A", 1.), ("Model B", 3.)])
        families.append({**cohort, "family_label": "Observed family", "family_weight": 1., "family_count": 100,
                         "family_median_mj_per_vkm": 1.2, "family_native_consumption": 3.6,
                         "empirical_observation_ids": audit})
        summaries.append({**cohort, "vehicle_group": group,
                          "production_mj_per_vkm": 1.2, "production_native_consumption": 3.6,
                          "catalogue_mj_per_vkm": 2., "catalogue_native_consumption": 6.,
                          "catalogue_model_min": 1., "catalogue_model_max": 3.,
                          "aggregation_method": "mto_identity_supported_weighted", "notes": audit})
    frames = {"composition_models": pd.DataFrame(models), "composition_families": pd.DataFrame(families),
              "composition_comparison": pd.DataFrame(summaries)}
    original = {key: frame.copy(deep=True) for key, frame in frames.items()}
    notebook = Path(__file__).resolve().parents[1] / "docs/insights/ldv_efficiency_diagnosis.py"
    cell = runpy.run_path(str(notebook))["composition_plot"]
    charts = []

    def chart_ui(chart):
        charts.append(chart.to_dict(validate=True))
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    years = list(range(2015, 2026))
    weights = pd.DataFrame([{"report_year": 2025, "weight_basis": "all_vintages",
        "nrcan_ceud_class": group, "nrcan_vehicle_class": size, "aggregation_weight": 1.}
        for group, size in [("Car", "Compact"), ("Light Truck", "Sport utility vehicle: Small")]])
    output, _ = cell.run(alt=alt, chart_ui=chart_ui,
        composition_class_columns=diagnostic.composition_class_columns, supported_display_years=diagnostic.supported_display_years,
        composition_view={"years": years, "top_size_classes": 5, "grid_plot_width": 1200,
            "class_order": {"Cars": ["Compact"], "Light trucks": ["Sport utility vehicle: Small"]}},
        diagnostic=SimpleNamespace(frames=frames, inputs={"rating_class_weights": weights}, summary={"base_year": 2023}),
        mo=mo, pd=pd, range_label=str, vehicle_group=SimpleNamespace(value=vehicle_group))
    assert len(output.text.encode("utf-8")) < 1_000_000
    assert len(charts) == 1
    rows = pd.DataFrame(charts[0]["data"]["values"])
    assert len(rows) == 7
    assert set(rows.year) == {2022}
    assert charts[0]["facet"]["column"]["field"] == "rating_class"
    assert charts[0]["facet"]["column"]["sort"] == ["Compact" if vehicle_group == "Cars" else "Sport utility vehicle: Small"]
    assert rows.loc[rows.evidence_kind.eq("Catalogue family median"), "value"].tolist() == [1., 3.]
    assert rows.loc[rows.evidence_kind.eq("Selected empirical description"), "family_weight"].item() == 1.
    assert rows.loc[rows.evidence_kind.eq("Production estimate"), "value"].item() == 1.2
    assert rows.loc[rows.evidence_kind.eq("Cohort fallback median"), "value"].item() == 2.
    aggregate = charts[0]["spec"]["layer"][-1]
    assert aggregate["mark"]["filled"] is False
    assert aggregate["encoding"]["size"]["condition"]["value"] == 180
    assert aggregate["encoding"]["size"]["value"] == 60
    assert aggregate["encoding"]["x"]["field"] == "value"
    assert aggregate["encoding"]["y"]["field"] == "model_year"
    assert aggregate["encoding"]["y"]["sort"] == ["2022"]
    assert aggregate["encoding"]["y"]["scale"]["domain"] == ["2022"]
    assert len(aggregate["encoding"]["x"]["axis"]["values"]) == 4
    assert aggregate["encoding"]["color"]["legend"]["orient"] == "bottom"
    kinds = ["Catalogue family median", "Selected empirical description", "Cohort fallback median", "Production estimate"]
    for channel in ["color", "shape"]:
        encoding = aggregate["encoding"][channel]
        assert encoding["title"] == "Evidence"
        assert encoding["scale"]["domain"] == kinds
        assert encoding["legend"]["columns"] == 4
        assert encoding["legend"]["direction"] == "horizontal"
        assert "values" not in encoding["legend"]
    assert aggregate["encoding"]["shape"]["scale"]["range"] == ["circle", "circle", "diamond", "cross"]
    assert charts[0]["spec"]["resolve"]["legend"]["size"] == "shared"
    assert charts[0]["spec"]["width"] == 1200
    assert aggregate["encoding"]["fill"]["condition"]["value"] == "#b7303e"
    assert aggregate["encoding"]["fill"]["value"] == "transparent"
    assert "xOffset" not in aggregate["encoding"]
    assert not {"rating_row_ids", "empirical_observation_ids", "notes"} & set(rows)
    for key, frame in original.items():
        pd.testing.assert_frame_equal(frames[key], frame)


def test_composition_columns_use_latest_pooled_mto_shares_across_years_and_powertrains():
    import altair as alt
    import marimo as mo

    years = list(range(2015, 2026))
    classes = ["Largest", "Second", "Third", "Fourth", "Fifth", "Excluded"]
    weights = pd.DataFrame([
        {"report_year": 2025, "weight_basis": "all_vintages", "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": size, "aggregation_weight": weight}
        for size, weight in zip(reversed(classes), [.01, .04, .1, .15, .3, .4], strict=True)
    ] + [
        {"report_year": report, "weight_basis": basis, "nrcan_ceud_class": group,
         "nrcan_vehicle_class": "Excluded", "aggregation_weight": 1.}
        for report, basis, group in [(2024, "all_vintages", "Car"), (2025, "model_year", "Car"),
                                     (2025, "all_vintages", "Light Truck")]
    ])
    cohorts = [{"year": year, "rating_class": size, "powertrain": powertrain, "native_unit": "L/100 km",
        "vehicle_group": "Cars"} for year, size, powertrain in [(2015, "Largest", "gasoline"),
        (2025, "Second", "phev_35_miles"), (2022, "Excluded", "gasoline"), (2014, "Largest", "gasoline")]]
    models = pd.DataFrame([{**cohort, "family_median_mj_per_vkm": 2., "model_native_consumption": 6.,
        "make_model": "Model " + cohort["rating_class"]} for cohort in cohorts])
    families = pd.DataFrame([{**cohort, "family_median_mj_per_vkm": 1.5, "family_native_consumption": 4.5,
        "family_label": "Observed " + cohort["rating_class"], "family_weight": 1., "family_count": 10} for cohort in cohorts])
    summaries = pd.DataFrame([{**cohort, "production_mj_per_vkm": 1.5, "production_native_consumption": 4.5,
        "catalogue_mj_per_vkm": 2., "catalogue_native_consumption": 6., "catalogue_model_min": 2.,
        "catalogue_model_max": 2., "aggregation_method": "izev_incentive_weighted"} for cohort in cohorts])
    frames = {"composition_models": models, "composition_families": families, "composition_comparison": summaries}
    notebook = Path(__file__).resolve().parents[1] / "docs/insights/ldv_efficiency_diagnosis.py"
    cell = runpy.run_path(str(notebook))["composition_plot"]
    charts = []
    output, _ = cell.run(alt=alt, chart_ui=lambda chart: charts.append(chart.to_dict(validate=True)) or mo.md("chart"),
        composition_class_columns=diagnostic.composition_class_columns, supported_display_years=diagnostic.supported_display_years,
        composition_view={"years": years, "top_size_classes": 5, "grid_plot_width": 1200, "class_order": {"Cars": classes[:5]}},
        diagnostic=SimpleNamespace(frames=frames, inputs={"rating_class_weights": weights}, summary={"base_year": 2023}),
        mo=mo, pd=pd, range_label=str, vehicle_group=SimpleNamespace(value="Cars"))
    assert len(charts) == 2
    assert "2025 MTO stock, all vintages" in output.text
    assert "99.0%" in output.text
    for chart in charts:
        assert chart["facet"]["column"]["sort"] == classes[:5]
        # Powertrain and measurement now share the facet header's title area.
        assert "title" not in chart
        assert "NRCan size class" in chart["facet"]["column"]["title"]
        target_year = 2015 if chart["facet"]["column"]["title"].startswith("gasoline ·") else 2025
        assert chart["spec"]["layer"][-1]["encoding"]["y"]["scale"]["domain"] == [str(target_year) + ("*" if target_year > 2023 else "")]
        rows = pd.DataFrame(chart["data"]["values"])
        assert set(rows.rating_class) == set(classes[:5])
        assert set(rows.year) == {target_year}
        missing = rows.loc[rows.evidence_kind.eq("Missing cohort")]
        assert len(missing) == 5
        actual = rows.loc[rows.evidence_kind.eq("Production estimate")]
        assert len(actual) == 1
        assert actual.value.item() == 1.5
        assert set(rows.loc[rows.evidence_kind.eq("Catalogue family median"), "label"]) <= {"Model Largest", "Model Second"}


@pytest.mark.parametrize("scope", ["stock_vehicle_group_share", "new_pev_powertrain_share",
                                  "new_pev_vehicle_group_share", "statcan_stock_powertrain_reference"])
def test_benchmark_chart_preserves_population_denominators_missing_cells_and_unpaired_years(scope):
    import altair as alt
    import marimo as mo

    source_ids = (["statcan_stock"] if scope == "statcan_stock_powertrain_reference" else
                  ["statcan_stock", "mto_stock"] if scope == "stock_vehicle_group_share" else ["statcan_new", "izev"])
    rows = []
    for region in ["ON", "QC"]:
        for year in [2023, 2024]:
            for source in source_ids:
                if year == 2024 and source == source_ids[0] and len(source_ids) == 2:
                    continue
                for group in ["Cars", "Light trucks"]:
                    for powertrain in (["all"] if scope == "stock_vehicle_group_share" else ["bev", "phev"]):
                        missing = year == 2024 and powertrain == "phev"
                        rows.append({"region": region, "year": year, "vehicle_group": group, "powertrain": powertrain,
                            "source": source, "count": 10 if region == "ON" else 900, "share": np.nan if missing else .25,
                            "denominator": 40 if region == "ON" else 3600, "comparison_scope": scope,
                            "population": "Explicit source population", "time_basis": "Request or registration calendar year",
                            "status": "missing_or_suppressed" if missing else "reported", "source_geography": region,
                            "denominator_basis": "Specific conditional population", "comparability_note": "Distinct populations remain explicit."})
    frame = pd.DataFrame(rows)
    original = frame.copy(deep=True)
    notebook = Path(__file__).resolve().parents[1] / "docs/insights/ldv_efficiency_diagnosis.py"
    cell = runpy.run_path(str(notebook))["composition_benchmark_plot"]
    charts, tables = [], []

    def chart_ui(chart):
        charts.append(chart.to_dict(validate=True))
        return mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)

    def table_ui(table, label):
        tables.append(table.copy())
        return mo.md(label)

    output, _ = cell.run(alt=alt, chart_ui=chart_ui, benchmark_scope=SimpleNamespace(value=scope),
        diagnostic=SimpleNamespace(frames={"composition_benchmark": frame}, summary={"region": "ON"}), mo=mo, table_ui=table_ui)
    assert "Distinct populations remain explicit" in output.text
    assert len(charts) == 1 and len(tables) == 2
    plotted = pd.DataFrame(charts[0]["data"]["values"])
    assert plotted["count"].eq(10).all()
    assert plotted.denominator.eq(40).all()
    assert set(plotted.year) == {2023, 2024}
    if scope != "stock_vehicle_group_share":
        assert plotted.share.isna().any()
    if len(source_ids) == 2:
        assert set(plotted.comparison_overlap) == {"Both sources present", "Source only or incomplete"}
    else:
        assert plotted.comparison_overlap.eq("StatCan reference only").all()
    assert tables[0]["Denominator count"].eq(40).all()
    assert tables[0]["Count"].eq(10).all()
    assert tables[0]["Share (%)"].dropna().eq(25).all()
    assert tables[1].region.eq("ON").all()
    assert tables[1].denominator_basis.eq("Specific conditional population").all()
    assert all(layer["encoding"]["y"]["scale"]["domain"] == [0, 1] for layer in charts[0]["spec"]["layer"])
    pd.testing.assert_frame_equal(frame, original)


def test_empty_benchmark_selection_displays_coverage_gap_without_a_zero_chart():
    import altair as alt
    import marimo as mo

    notebook = Path(__file__).resolve().parents[1] / "docs/insights/ldv_efficiency_diagnosis.py"
    cell = runpy.run_path(str(notebook))["composition_benchmark_plot"]
    frame = pd.DataFrame(columns=["region", "comparison_scope"])
    output, _ = cell.run(alt=alt, chart_ui=lambda _: pytest.fail("empty comparison became a chart"),
        benchmark_scope=SimpleNamespace(value="stock_vehicle_group_share"),
        diagnostic=SimpleNamespace(frames={"composition_benchmark": frame}, summary={"region": "ON"}),
        mo=mo, table_ui=lambda *_: pytest.fail("empty comparison became observations"))
    assert "No captured observations support this comparison" in output.text
