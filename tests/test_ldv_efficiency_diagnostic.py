"""Source distributions, class resolution and recorded efficiency bridge contracts."""

from dataclasses import replace
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


def test_filter_evidence_compares_same_native_cohorts_before_and_after(bundle):
    source = ratings().assign(included_before_filter=True, outlier_excluded=False,
                             filter_reason="", outlier_status="within_fences")
    source.loc[source.Model.eq("B"), ["included", "outlier_excluded", "filter_reason", "outlier_status"]] = [False, True, "Above upper fence", "above_upper_fence"]
    original = source.copy(deep=True)
    audit_rows, impact = diagnostic.rating_filter_evidence(bundle, source)
    compact = impact.loc[impact.rating_class.eq("Compact")].iloc[0]
    assert compact.before_distinct_models == 3 and compact.after_distinct_models == 2
    assert compact.before_catalogue_rows == 4 and compact.after_catalogue_rows == 3
    assert compact.before_distinct_model_mean == pytest.approx((15 + 100 + 40) / 3)
    assert compact.after_distinct_model_mean == compact.after_distinct_model_median == pytest.approx(27.5)
    assert audit_rows.loc[audit_rows.Model.eq("B"), "filter_reason"].tolist() == ["Above upper fence"]
    pd.testing.assert_frame_equal(source, original)


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


def test_native_phev_keeps_cs_and_cd_means_distinct(bundle, monkeypatch):
    rows, _ = phev_ratings_and_weights(bundle)
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
    assert set(harmonised.series) == {"NRCan size-class aggregate", "NRCan CS fuel-only aggregate",
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
