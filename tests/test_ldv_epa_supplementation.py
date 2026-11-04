"""Read-only EPA screens preserve units, observation mass and held-out groups."""

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from validation.ldv_epa_supplementation import (
    candidate_benefit_bounds, catalogue_pairs, cohort_sensitivity, coverage_tables, holdout_validation, load_report, observation_dispositions, prepare_epa, range_bucket,
)


@pytest.fixture
def conversions():
    return {
        "length": {"mile_to_km": 1.609344}, "volume": {"us_gallon_to_litre": 3.785411784},
        "energy": {"kwh_to_mj": 3.6, "wh_to_mj": .0036, "wh_per_gge": 33700.},
        "fuel_hhv": {"gasoline_mj_per_litre": 33.568202923012876, "diesel_mj_per_litre": 38.59941622386618},
    }


@pytest.fixture
def ranges():
    return {"buckets": {"BEV": [
        {"sub_category": "bev_150_miles", "upper": 150, "upper_inclusive": True},
        {"sub_category": "bev_200_miles", "upper": 250, "upper_inclusive": True},
        {"sub_category": "bev_300_miles", "upper": 350, "upper_inclusive": True},
        {"sub_category": "bev_400_miles", "upper": None, "upper_inclusive": True},
    ], "PHEV": [
        {"sub_category": "phev_35_miles", "upper": 35, "upper_inclusive": False},
        {"sub_category": "phev_50_miles", "upper": None, "upper_inclusive": False},
    ]}}


def native_epa(**overrides):
    return dict(id=1, year=2023, make="Nissan", model="LEAF", VClass="Midsize Cars", atvType="EV",
        fuelType1="Electricity", fuelType2=np.nan, comb08=100, comb08U=100., combA08U=0.,
        combE=33.7, combinedCD=0., phevBlended=False, range=150, rangeA=0,
        cylinders=0., displ=0., trany="Automatic (A1)", baseModel="LEAF", drive="Front-Wheel Drive") | overrides


def test_native_electricity_and_mpge_share_energy_boundary_without_hhv_rescaling(conversions, ranges):
    raw = pd.DataFrame([native_epa()])
    result = prepare_epa(raw, {}, {"Midsize Cars": "Mid-size"}, ranges, conversions).iloc[0]

    assert result.epa_mj_per_vkm == pytest.approx(33.7 * 3.6 / 160.9344)
    assert result.label_equivalent_cd_mj_per_vkm == pytest.approx(result.epa_mj_per_vkm)
    assert result.native_label_fraction == pytest.approx(0.)
    assert result.powertrain == "bev_150_miles"
    assert pd.isna(result.cs_fuel_mj_per_vkm)
    assert pd.isna(result.cd_fuel_mj_per_vkm)
    assert "epa_mj_per_vkm" not in raw


def test_replay_context_uses_captured_atb_factors_and_reviewed_reference(tmp_path, monkeypatch):
    from validation import ldv_epa_supplementation as module

    path = tmp_path / "atb.csv"
    pd.DataFrame({"vehicle_weight_category": ["LDV", "LDV", "LDV", "MDV"],
        "electric_range_mi": [35, 35, 50, 35], "fleet_utility_factor": [.57, .57, .69, .1]}).to_csv(path, index=False)
    bundle, reference, ranges, conversions = object(), object(), object(), {"captured": True}
    recorded = SimpleNamespace(bundle=bundle, paths={"atb_phev": path})
    rules = {"atb": {"weight_categories": {"ldv": "LDV"}}, "ldv_archetypes": {
        "phev_35_miles": {"range_mi": 35}, "phev_50_miles": {"range_mi": 50}, "gasoline": {}}}
    monkeypatch.setattr(module, "load_vehicle_reference", lambda selected: reference if selected is bundle else None)
    monkeypatch.setattr(module, "load_range_rules", lambda selected: ranges if selected is bundle else None)
    context = module._composition_context(recorded, rules, conversions)
    assert context["reference"] is reference and context["range_rules"] is ranges
    assert context["conversions"] is conversions
    assert context["uf_by_powertrain"] == {"phev_35_miles": .57, "phev_50_miles": .69}


@pytest.mark.parametrize("values", [[], [.4, .6], [float("nan")], [1.2]])
def test_replay_context_rejects_missing_or_conflicting_captured_utility_factors(tmp_path, values):
    from validation import ldv_epa_supplementation as module

    path = tmp_path / "atb.csv"
    pd.DataFrame([{"vehicle_weight_category": "LDV", "electric_range_mi": 35, "fleet_utility_factor": value}
        for value in values], columns=["vehicle_weight_category", "electric_range_mi", "fleet_utility_factor"]).to_csv(path, index=False)
    recorded = SimpleNamespace(paths={"atb_phev": path})
    rules = {"atb": {"weight_categories": {"ldv": "LDV"}}, "ldv_archetypes": {"phev_35_miles": {"range_mi": 35}}}
    with pytest.raises(ValueError, match="Captured ATB range utility factor"):
        module._composition_context(recorded, rules, {})


def test_mpg_uses_fuel_hhv_and_unrounded_adjusted_mpg(conversions, ranges):
    raw = pd.DataFrame([native_epa(atvType="", fuelType1="Regular Gasoline", comb08=30, comb08U=30.4)])
    result = prepare_epa(raw, {}, {}, ranges, conversions).iloc[0]

    assert result.epa_mj_per_vkm == pytest.approx(3.785411784 * 33.568202923012876 / (30.4 * 1.609344))
    assert result.mpg_basis == "unrounded_adjusted"


def test_missing_blended_cd_fuel_is_not_inferred_from_mpge(conversions, ranges):
    raw = pd.DataFrame([native_epa(atvType="Plug-in Hybrid", fuelType1="Premium Gasoline",
        fuelType2="Electricity", phevBlended=True, combinedCD=np.nan, combA08U=80.)])
    result = prepare_epa(raw, {}, {}, ranges, conversions).iloc[0]

    assert pd.isna(result.cd_fuel_mj_per_vkm)
    assert pd.isna(result.epa_mj_per_vkm)  # Requires an independently supplied CANOE UF.


def test_unknown_blend_flag_does_not_imply_zero_cd_fuel(conversions, ranges):
    raw = pd.DataFrame([native_epa(atvType="Plug-in Hybrid", fuelType1="Regular Gasoline",
        fuelType2="Electricity", phevBlended=np.nan, combinedCD=np.nan)])
    result = prepare_epa(raw, {}, {}, ranges, conversions).iloc[0]
    assert pd.isna(result.cd_fuel_mj_per_vkm)


def test_phev_screen_uses_cs_fuel_grade_and_canoe_utility_factor(conversions, ranges):
    expected = .8 * 15 * 3.6 / 100 + .2 * 6 * conversions["fuel_hhv"]["gasoline_mj_per_litre"] / 100
    row = dict(included=True, year=2023, Make="Example", Model="PHEV", rating_class="Mid-size",
        powertrain="phev_35_miles", native_unit="Le/100 km", component="phev", source_row=2,
        native_consumption=2., consumption_mj_per_vkm=expected, range_km=50.,
        fleet_utility_factor=.8, cd_electricity_kwh_per_100km=15., cd_fuel_l_per_100km=0., cs_fuel_l_per_100km=6.)
    row.update({"Engine size (L)": 2., "Cylinders": 4., "Transmission": "AV", "Fuel type": np.nan,
                "Fuel type 1": "B/X", "Fuel type 2": "X"})
    spec = {"variant_identity_columns": ["Model", "Engine size (L)", "Cylinders", "Transmission", "Fuel type 2"]}
    epa = prepare_epa(pd.DataFrame([native_epa(make="Example", model="PHEV", baseModel="PHEV", atvType="Plug-in Hybrid",
        fuelType1="Regular Gasoline", fuelType2="Electricity", comb08U=235.2145833333333 / 6,
        combE=15 * 1.609344, combinedCD=0., phevBlended=False, rangeA=50 / 1.609344,
        cylinders=4., displ=2., trany="Automatic (variable gear ratios)")]), spec,
        {"Midsize Cars": "Mid-size"}, ranges, conversions)

    pair = catalogue_pairs(pd.DataFrame([row]), epa, spec, conversions).iloc[0]

    assert pair.reciprocal_unique_screen
    assert pair.epa_mj_per_vkm == pytest.approx(expected)
    assert pair.relative_difference == pytest.approx(0.)
    assert not pair.physical_correspondence_verified


@pytest.mark.parametrize("miles,family,expected", [
    (150., "bev", "bev_150_miles"), (150.00001, "bev", "bev_200_miles"),
    (34.99999, "phev", "phev_35_miles"), (35., "phev", "phev_50_miles"),
    (0., "bev", ""), (np.nan, "phev", ""),
])
def test_captured_range_edges_are_preserved(ranges, miles, family, expected):
    assert range_bucket(miles, family, ranges) == expected


def observation(**overrides):
    return dict(region="ON", year=2023, source_kind="izev", family_make="Nissan", family_model="LEAF",
        observed_powertrain="bev", rating_class="", source_count=294, selected=False,
        match_status="unmatched_ratings", identity_supported=True, transaction_eligible=True,
        composition_basis="observed_model_year", pool_geography="provincial", pool_provinces="ON") | overrides


def test_multiple_epa_trims_do_not_multiply_mass_or_resolve_unobserved_range():
    observed = pd.DataFrame([observation()])
    ratings = pd.DataFrame([dict(included=True, year=2022, Make="Nissan", Model="LEAF", powertrain="bev_150_miles")])
    epa = pd.DataFrame([
        dict(year=2023, make_key="NISSAN", model="LEAF", family="bev", rating_class="Mid-size", powertrain="bev_150_miles", epa_mj_per_vkm=.7),
        dict(year=2023, make_key="NISSAN", model="LEAF SV", family="bev", rating_class="Mid-size", powertrain="bev_200_miles", epa_mj_per_vkm=.71),
    ])
    result = observation_dispositions(observed, ratings, epa, {}).iloc[0]

    assert result.source_count == 294
    assert result.epa_candidate_rows == 2
    assert result.coverage_disposition == "missing_own_year_nrcan_family_rating"
    assert result.candidate_recovery_upper_bound == 0
    assert result.accepted_recovery_count == 0


def test_existing_nrcan_trim_and_unverified_mto_identity_are_not_rating_gaps():
    observed = pd.DataFrame([observation(source_kind="mto", family_make="Dodge", family_model="Caravan",
        observed_powertrain="", rating_class="Minivan", identity_supported=False, source_count=100)])
    ratings = pd.DataFrame([dict(included=True, year=2023, Make="Dodge", Model="Grand Caravan", powertrain="gasoline")])
    epa = pd.DataFrame([dict(year=2023, make_key="DODGE", model="Caravan", family="gasoline", rating_class="Minivan", powertrain="gasoline", epa_mj_per_vkm=3.)])
    result = observation_dispositions(observed, ratings, epa, {}).iloc[0]

    assert result.candidate_recovery_upper_bound == 0
    assert result.accepted_recovery_count == 0


def test_interior_whole_token_nrcan_name_is_a_review_lead_not_an_epa_gap():
    observed = pd.DataFrame([observation(source_kind="mto", family_make="Volkswagen", family_model="GTI",
        year=2015, observed_powertrain="", rating_class="Compact", identity_supported=True, source_count=1526)])
    ratings = pd.DataFrame([
        dict(included=True, year=2012, Make="Volkswagen", Model="GTI", powertrain="gasoline"),
        dict(included=True, year=2015, Make="Volkswagen", Model="Golf GTI", powertrain="gasoline"),
    ])
    epa = pd.DataFrame([dict(year=2015, make_key="VOLKSWAGEN", model="GTI", family="gasoline",
        rating_class="Compact", powertrain="gasoline", epa_mj_per_vkm=3.)])

    result = observation_dispositions(observed, ratings, epa, {}).iloc[0]

    assert result.coverage_disposition == "existing_nrcan_name_or_trim_ambiguity"
    assert result.nrcan_neighbour_models == "Golf GTI"
    assert result.candidate_recovery_upper_bound == 0
    assert result.accepted_recovery_count == 0


def test_pool_denominator_counts_each_description_once_and_labels_proxy_reuse():
    observations = pd.DataFrame([
        observation(selected=True, match_status="resolved", source_count=40),
        observation(family_model="ARIYA", source_count=60),
        observation(composition_basis="historical_pooled_model_year_window", year=2015, source_count=100),
    ])
    observations["candidate_recovery_upper_bound"] = 0
    observations["pre_range_candidate_count"] = 0
    observations["accepted_recovery_count"] = 0
    classes = pd.DataFrame([dict(region="ON", year=2023, rating_class="Mid-size", powertrain="bev_150_miles",
        selected_source_count=40, class_mean_mj_per_vkm=.7)])
    pools, cohorts = coverage_tables(observations, classes)
    current = pools.loc[pools.composition_basis.eq("observed_model_year")].iloc[0]
    historical = pools.loc[pools.composition_basis.eq("historical_pooled_model_year_window")].iloc[0]

    assert current.denominator_count == 100
    assert current.existing_selected_count == 40
    assert current.existing_selected_share == .4
    assert "do not sum across years" in historical.additivity_note
    assert cohorts.accepted_class_change_mj_per_vkm.eq(0).all()


def test_holdout_excludes_entire_family_across_years_and_entire_year():
    pairs = pd.DataFrame([
        dict(reciprocal_unique_screen=True, family="gasoline", regime="2017_onward", family_holdout=family,
            year=year, epa_mj_per_vkm=ratio, consumption_mj_per_vkm=1., rating_variant_id=f"{family}{year}", id=index)
        for index, (family, year, ratio) in enumerate([("A", 2022, 1.2), ("A", 2023, 1.4), ("B", 2022, .8), ("B", 2023, .9)])
    ])
    result = holdout_validation(pairs)
    family_a = result.loc[result.holdout.eq("family") & result.held_out_group.eq("A")]
    year_2022 = result.loc[result.holdout.eq("model_year") & result.held_out_group.eq("2022")]

    assert len(family_a) == 2
    assert np.allclose(family_a.training_epa_over_nrcan_ratio, .85)
    assert family_a.train_pairs.eq(2).all()
    assert family_a.train_families.eq(1).all()
    assert np.allclose(year_2022.training_epa_over_nrcan_ratio, 1.15)
    assert year_2022.train_years.eq(1).all()
    assert result.physical_correspondence_verified.eq(False).all()


def test_duplicate_source_rows_share_one_screened_variant_without_losing_replacement():
    classes = pd.DataFrame([dict(region="ON", year=2023, rating_class="Mid-size", powertrain="bev_150_miles",
        ceud_class="Car", class_mean_mj_per_vkm=.6, selected_source_count=10, weight=1.)])
    observations = pd.DataFrame([observation(selected=True, source_count=10,
        empirical_observation_id="description", rating_class="Mid-size", resolved_powertrain="bev_150_miles",
        observed_family_mj_per_vkm=.6)])
    variants = pd.DataFrame([dict(region="ON", empirical_observation_id="description", rating_variant_id="variant",
        component="bev", source_row=number, family_variant_count=1, source_rows_per_variant=2) for number in [1, 2]])
    pairs = pd.DataFrame([dict(reciprocal_unique_screen=True, rating_variant_id="variant", epa_mj_per_vkm=.7)])

    result = cohort_sensitivity(classes, observations, variants, pairs).iloc[0]

    assert result.empirical_count == 10
    assert result.screened_replacement_count == 10
    assert result.screened_sensitivity_mj_per_vkm == pytest.approx(.7)
    assert result.nrcan_mj_per_vkm == .6
    assert result.accepted_recovered_count == 0


def test_unavailable_cng_total_energy_does_not_become_zero():
    classes = pd.DataFrame([dict(region="ON", year=2000, rating_class="Compact", powertrain="cng", ceud_class="Car",
        class_mean_mj_per_vkm=np.nan, selected_source_count=0, weight=1.)])
    observations = pd.DataFrame(columns=["selected", "observed_family_mj_per_vkm", "region", "empirical_observation_id",
        "year", "rating_class", "resolved_powertrain", "source_count"])
    observations["selected"] = observations.selected.astype(bool)
    variants = pd.DataFrame(columns=["region", "empirical_observation_id", "rating_variant_id"])
    pairs = pd.DataFrame(columns=["reciprocal_unique_screen", "rating_variant_id", "epa_mj_per_vkm"])
    pairs["reciprocal_unique_screen"] = pairs.reciprocal_unique_screen.astype(bool)

    result = cohort_sensitivity(classes, observations, variants, pairs).iloc[0]

    assert pd.isna(result.nrcan_mj_per_vkm)
    assert pd.isna(result.accepted_supplement_mj_per_vkm)
    assert pd.isna(result.accepted_supplement_fraction)
    assert not result.total_energy_available


def test_missing_report_has_explicit_reproduction_command(tmp_path, monkeypatch):
    from validation import ldv_epa_supplementation

    monkeypatch.setattr(ldv_epa_supplementation, "_report_directory", lambda root: tmp_path)
    with pytest.raises(FileNotFoundError, match="--write-report"):
        load_report()


def test_report_reader_rejects_tampered_exports_and_changed_analysis(tmp_path, monkeypatch):
    from utils import file_sha256
    from validation import ldv_epa_supplementation as module

    owner = tmp_path / "src/parameterization/rating_composition.py"
    owner.parent.mkdir(parents=True)
    owner.write_text("# captured owning implementation\n", encoding="utf-8")
    reference_owner = owner.with_name("vehicle_reference.py")
    range_owner = owner.with_name("ldv_ev_ranges.py")
    reference_owner.write_text("# captured vehicle reference\n", encoding="utf-8")
    range_owner.write_text("# captured range allocation\n", encoding="utf-8")
    cache = tmp_path / "pinned.zip"
    cache.write_bytes(b"pinned")
    table = tmp_path / "table.csv"
    table.write_text("count\n12\n", encoding="utf-8")
    adapter = {"cache_path": "pinned.zip", "expected_sha256": file_sha256(cache)}
    component = SimpleNamespace(adapter=adapter)
    sources = SimpleNamespace(sources={"fueleconomy_gov_vehicle_data": SimpleNamespace(components={"vehicles": component})})
    recorded = SimpleNamespace(metadata={"build_id": "build"}, bundle=SimpleNamespace(repo_root=tmp_path, sources=sources), paths={})
    summary = {"build": {"build_id": "build"}, "regions": ["ON"], "analysis_code": {
        "validation.ldv_epa_supplementation": file_sha256(Path(module.__file__)),
        "parameterization.rating_composition": file_sha256(owner),
        "parameterization.vehicle_reference": file_sha256(reference_owner),
        "parameterization.ldv_ev_ranges": file_sha256(range_owner)}, "evidence_inputs": {},
        "epa_input": {"sha256": file_sha256(cache)}, "exports": {"table": {"file": "table.csv", "rows": 1, "sha256": file_sha256(table)}}}
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    monkeypatch.setattr(module, "_report_directory", lambda root: tmp_path)
    monkeypatch.setattr(module, "load_notebook_evidence", lambda *args, **kwargs: recorded)
    monkeypatch.setattr(module, "_configured_regions", lambda root: ("ON",))
    monkeypatch.setattr(module, "resolve_input_path", lambda *args: cache)

    assert load_report().frames["table"].iloc[0]["count"] == 12
    monkeypatch.setattr(module, "_configured_regions", lambda root: ("BCT",))
    with pytest.raises(ValueError, match="diagnostic regions changed"):
        load_report()
    monkeypatch.setattr(module, "_configured_regions", lambda root: ("ON",))
    table.write_text("count\n13\n", encoding="utf-8")
    with pytest.raises(ValueError, match="export table changed"):
        load_report()
    table.write_text("count\n12\n", encoding="utf-8")
    owner.write_text("# changed implementation\n", encoding="utf-8")
    with pytest.raises(ValueError, match="code changed"):
        load_report()


def test_candidate_gate_crossing_reuses_existing_distinct_variant_family_means():
    observations = pd.DataFrame([dict(region="ON", year=2007, source_kind="mto", family_make="Example", family_model="SUV",
        epa_candidate_classes="Sport utility vehicle", epa_candidate_powertrains="gasoline", source_count=2,
        candidate_recovery_upper_bound=2, candidate_consumption_min_mj_per_vkm=1.5, candidate_consumption_max_mj_per_vkm=2.5)])
    classes = pd.DataFrame([
        dict(region="ON", year=2007, rating_class="Sport utility vehicle", powertrain="gasoline", ceud_class="Light Truck",
            resolved_gasoline_count=100, identity_supported_count=29, minimum_resolved_count_share=.3, class_mean_mj_per_vkm=2., weight=.4),
        dict(region="ON", year=2007, rating_class="Pickup truck: Standard", powertrain="gasoline", ceud_class="Light Truck",
            resolved_gasoline_count=100, identity_supported_count=40, minimum_resolved_count_share=.3, class_mean_mj_per_vkm=3., weight=.6),
    ])
    matches = pd.DataFrame([dict(region="ON", year=2007, rating_class="Sport utility vehicle", source_kind="mto",
        identity_supported=True, match_status="resolved", assigned_powertrain="gasoline", empirical_observation_id=identity,
        rating_variant_id=variant, consumption_mj_per_vkm=mean, source_count=count)
        for identity, variant, mean, count in [("A", "v1", 1., 20), ("A", "v1", 1., 20), ("B", "v2", 3., 9)]])

    result = candidate_benefit_bounds(observations, classes, matches).iloc[0]

    assert result.gate_crossed
    assert result.optimistic_identity_supported_share == pytest.approx(31 / 102)
    assert result.optimistic_selected_count == 31
    assert result.existing_documentary_family_mean_mj_per_vkm == pytest.approx(47 / 29)
    assert result.optimistic_class_min_mj_per_vkm == pytest.approx(50 / 31)
    assert result.optimistic_class_max_mj_per_vkm == pytest.approx(52 / 31)
    assert result.optimistic_cohort_min_mj_per_vkm == pytest.approx(2.6 + .4 * (50 / 31 - 2.))
    assert result.accepted_recovered_count == 0


def test_candidate_resolution_below_gate_keeps_catalogue_median():
    observations = pd.DataFrame([dict(region="ON", year=2008, source_kind="mto", family_make="Example", family_model="Sedan",
        epa_candidate_classes="Full-size", epa_candidate_powertrains="gasoline", source_count=3, candidate_recovery_upper_bound=3)])
    classes = pd.DataFrame([dict(region="ON", year=2008, rating_class="Full-size", powertrain="gasoline", ceud_class="Car",
        resolved_gasoline_count=1000, identity_supported_count=1, minimum_resolved_count_share=.3, class_mean_mj_per_vkm=2., weight=1.)])

    result = candidate_benefit_bounds(observations, classes, pd.DataFrame()).iloc[0]

    assert not result.optimistic_gate_met
    assert result.optimistic_selected_count == 0
    assert result.optimistic_class_min_mj_per_vkm == result.optimistic_class_max_mj_per_vkm == 2.
    assert result.optimistic_cohort_min_fraction == result.optimistic_cohort_max_fraction == 0.
