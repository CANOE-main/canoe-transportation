"""Counted families determine consumption without inventing unobserved shares."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import yaml

from parameterization.rating_composition import (
    aggregate_empirical_ratings,
    canonical_family,
    normalize_family,
    catalogue_family, catalogue_family_labels, median_support_weights,
)


@pytest.fixture
def policy():
    return {
        "izev_model_year_min": 2020, "izev_model_year_max": 2024,
        "incumbent_powertrains": ["gasoline"],
        "empirical_subclasses": "median_fill_original_weights",
        "izev_transactions": {"column": "Purchase or Lease", "accepted_values": ["Purchase", "Lease"]},
        "family_statistic": "median", "variant_allocation": "family_median",
        "family_matching": {"minimum_similarity": .9, "minimum_family_margin": .08,
            "trim_suffix_patterns": [r"(?i)\s+(?:AWD|FWD|RWD|4WD|4X4|Sport|Standard|Long Range|Pro|Turbo|Performance|Preferred)(?:\s+.*)?$"],
            "family_patterns": [
                {"make": "Tesla", "pattern": r"(?i)^Model\s*3(?:\s+.*)?$", "canonical_model": "Model 3"},
                {"make": "Tesla", "pattern": r"(?i)^Model\s*Y(?:\s+.*)?$", "canonical_model": "Model Y"},
                {"make": "Audi", "pattern": r"(?i)^Q4(?:\s+(?:45|50|55))?\s+e-tron(?:\s+quattro)?$", "canonical_model": "Q4 e-tron"},
            ]},
        "izev_matching": {"trim_priority": "explicit_configuration_then_family_median", "range_assignment": "family_median"},
        "izev_historical_composition": {"method": "pooled_supported_model_year_window"},
        "mto_selection": {
            "method": "identity_supported_coverage", "minimum_resolved_count_share": .3,
            "direct_methods": ["automatic_exact_normalized_make_exact_normalized_model",
                               "automatic_configured_make_alias_exact_normalized_model",
                               "automatic_approved_current_candidate_approved_similarity_exact_normalized"],
            "external_methods": ["reviewed_external_model_vin_family_association"],
        },
        "izev_pool_selection": {"method": "provincial_observation_count", "minimum_observations": 1,
                                "national_provinces": ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"]},
        "aliases": [{"make": "Mazda", "canonical_model": "Mazda3", "models": ["3", "Mazda3"]}],
        "variant_identity_columns": ["Model", "Engine size (L)", "native_consumption", "range_km"],
    }


@pytest.fixture(scope="module")
def production_family_policy():
    """Exercise reviewed runtime family patterns rather than copying their regexes."""
    path = Path(__file__).resolve().parents[1] / "config" / "parameters" / "rules.yaml"
    rules = yaml.safe_load(path.read_text(encoding="utf-8"))
    return rules["parameterization"]["efficiencies"]["ratings"]["composition"]


def ratings(models, *, year=2023, make="Fixture", powertrain="gasoline", rating_class="Compact"):
    return pd.DataFrame([
        {"source_id": "ratings", "component": "edition", "source_row": index + 2,
         "year": year, "Make": make, "Model": model, "powertrain": powertrain,
         "rating_class": rating_class, "included": True,
         "native_consumption": value, "consumption_mj_per_vkm": value * .4,
         "range_km": {"bev_150_miles": 200., "bev_200_miles": 350., "bev_300_miles": 500., "bev_400_miles": 650.,
                      "phev_35_miles": 40., "phev_50_miles": 70.}.get(powertrain, np.nan),
         "native_unit": "kWh/100 km" if powertrain.startswith("bev") else "L/100 km"}
        for index, (model, value) in enumerate(models)
    ])


def stock(models, *, year=2023, make="Fixture", rating_class="Compact"):
    return pd.DataFrame([
        {"report_year": 2025, "MODEL_YEAR": year, "MAKE": make,
         "MODEL": f"code{index}", "FIT_ACTIVE": count, "mapping_accepted": True,
         "match_method": "automatic_exact_normalized_make_exact_normalized_model",
         "canonical_make": make, "canonical_model": model, "nrcan_vehicle_class": rating_class}
        for index, (model, count) in enumerate(models)
    ])


def incentives(models, *, province="ON", year=2023, make="Fixture", powertrain="bev"):
    return pd.DataFrame([
        {"model_year": year, "request_year": year + 1, "province": province,
         "make": make, "model": model, "powertrain": powertrain, "count": count,
         "source_row": index + 2, "Purchase or Lease": "Purchase"}
        for index, (model, count) in enumerate(models)
    ])


def range_rules():
    return SimpleNamespace(buckets={"BEV": [
        SimpleNamespace(sub_category="bev_150_miles", upper=150., upper_inclusive=True),
        SimpleNamespace(sub_category="bev_200_miles", upper=250., upper_inclusive=True),
        SimpleNamespace(sub_category="bev_300_miles", upper=350., upper_inclusive=True),
        SimpleNamespace(sub_category="bev_400_miles", upper=None, upper_inclusive=True),
    ], "PHEV": [SimpleNamespace(sub_category="phev_35_miles", upper=35., upper_inclusive=False),
                SimpleNamespace(sub_category="phev_50_miles", upper=None, upper_inclusive=False)]})


def conversion_factors():
    return {"length": {"mile_to_km": 1.609344}, "energy": {"kwh_to_mj": 3.6},
            "service": {"per_100_km_to_per_km": .01},
            "fuel_hhv": {"gasoline_mj_per_litre": 33.568202923012876}}


def aggregate(frame, policy, *, mto=None, izev=None, regions=None, weights=None):
    if weights is None:
        weights = pd.DataFrame([
            {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Car",
             "nrcan_vehicle_class": "Compact", "aggregation_weight": 1.0},
        ])
    return aggregate_empirical_ratings(
        frame, weights, pd.DataFrame() if mto is None else mto,
        pd.DataFrame() if izev is None else izev,
        rules={"ratings": {"fields": {"make": "Make", "model": "Model"}},
               "historical_suv": {"source_label": "SUV", "combined_weight_classes": ["Small SUV", "Large SUV"]},
               "tolerances": {"weight_sum": 1e-9}},
        range_rules=range_rules(), conversions=conversion_factors(), uf_by_powertrain={"phev_35_miles": .5, "phev_50_miles": .7},
        composition_rules=policy, regions=regions or ["ON"], weight_basis="all_vintages",
    )


def test_empirical_family_counts_survive_trim_and_catalogue_duplicates(policy):
    frame = ratings([("A", 5), ("A Sport", 9), ("A", 5), ("B", 15)])
    result = aggregate(frame, policy, mto=stock([("A", 90), ("B", 10)]))
    row = result.class_audit.iloc[0]
    assert row.native_class_mean == pytest.approx(.9 * 7 + .1 * 15)
    assert row.class_mean_mj_per_vkm == pytest.approx(row.native_class_mean * .4)
    assert row.aggregation_method == "mto_identity_supported_weighted"
    assert row.coverage_status == "conditional_identity_supported_coverage"
    assert result.variants.variant_weight.sum() == pytest.approx(1)
    assert result.variants.allocated_count.sum() == pytest.approx(100)
    assert result.variants.groupby("empirical_family_model").variant_weight.sum().to_dict() == pytest.approx({"A": .9, "B": .1})
    assert result.variants.groupby("rating_variant_id").variant_weight.sum().tolist() == pytest.approx([.45, .45, .1])
    assert set(result.variants.source_row) == {2, 3, 4, 5}


def test_stale_mto_family_counts_cannot_weight_a_different_class_edition(policy):
    mapped = stock([("A", 100)])
    mapped["report_year"] = 2024
    with pytest.raises(ValueError, match="different report editions"):
        aggregate(ratings([("A", 7)]), policy, mto=mapped)


def test_aliases_merge_identity_before_selection_and_preserve_raw_keys(policy):
    mapped = stock([("3", 88_035), ("Mazda3", 38_174)], make="Mazda")
    result = aggregate(ratings([("Mazda3 4-Door", 7), ("Mazda3 5-Door", 8)], make="Mazda"), policy, mto=mapped)
    observed = result.observations.iloc[0]
    assert len(result.observations) == 1
    assert observed.source_count == 126_209
    assert observed.duplicate_identity_merged
    assert observed.raw_model == "3 | Mazda3"
    assert observed.raw_observation_ids == "Mazda/code0 | Mazda/code1"
    assert result.class_audit.native_class_mean.item() == 7.5
    assert canonical_family("Mazda", "3", policy) == ("Mazda", "Mazda3")
    assert normalize_family("MAZDA", "MAZDA3", policy) == ("MAZDA", "MAZDA3")


def test_family_prefix_does_not_match_unrelated_numeric_or_model_suffix(policy):
    frame = ratings([("Model 3", 5), ("Model 30", 50), ("FalconX", 100)])
    result = aggregate(frame, policy, mto=stock([("Model 3", 80), ("Falcon", 20)]))
    assert result.class_audit.native_class_mean.item() == 5
    assert result.observations.set_index("family_model").match_status.to_dict() == {
        "Falcon": "unmatched_ratings", "Model 3": "resolved",
    }
    assert result.class_audit.matched_source_share.item() == .8


def test_mto_family_proxy_uses_only_compatible_gasoline_ratings(policy):
    gas = ratings([("Shared", 5), ("Gas only", 10), ("Hybrid only", 7)])
    gas.loc[gas.Model.eq("Hybrid only"), "powertrain"] = "hev"
    hybrid = ratings([("Shared Hybrid", 3)], powertrain="hev")
    hybrid.source_row += 100
    result = aggregate(pd.concat([gas, hybrid], ignore_index=True), policy,
                       mto=stock([("Shared", 1000), ("Gas only", 90), ("Hybrid only", 50)]))
    status = result.observations.set_index("family_model").match_status
    assert status["Shared"] == "resolved"
    assert status["Hybrid only"] == "unmatched_ratings"
    assert result.class_audit.set_index("powertrain").loc["gasoline", "native_class_mean"] == pytest.approx((1000 * 5 + 90 * 10) / 1090)
    assert result.class_audit.set_index("powertrain").loc["gasoline", "propulsion_basis"] == "canonical_family_gasoline_compatibility_not_observed_fuel"
    assert result.class_audit.set_index("powertrain").loc["hev", "aggregation_method"] == "catalogue_distinct_model_median"


def test_exact_vehicle_year_and_class_never_borrow_adjacent_observations(policy):
    early = ratings([("A", 5)], year=2022)
    late = ratings([("A", 10)], year=2023)
    late.source_row += 100
    result = aggregate(pd.concat([early, late], ignore_index=True), policy, mto=stock([("A", 100)], year=2023))
    methods = result.class_audit.set_index("year").aggregation_method.to_dict()
    assert methods[2022] == "catalogue_fallback_median_no_empirical_support"
    assert methods[2023] == "mto_identity_supported_weighted"
    assert set(result.variants.year) == {2023}


def test_all_identity_supported_families_contribute_without_rank_or_count_cap(policy):
    models = [(f"M{number}", 5 + number) for number in range(8)]
    counts = [(f"M{number}", count) for number, count in enumerate([50, 20, 10, 6, 5, 4, 3, 2])]
    result = aggregate(ratings(models), policy, mto=stock(counts))
    selected = result.observations.loc[result.observations.selected]
    assert selected.family_model.tolist() == [f"M{number}" for number in range(8)]
    assert result.class_audit.selected_resolved_share.item() == 1
    assert result.class_audit.native_class_mean.item() == pytest.approx(6.3)
    assert selected.relative_contribution.sum() == pytest.approx(1)
    assert result.class_audit.largest_family_share.item() == .5
    assert result.class_audit.effective_family_count.item() == pytest.approx(1 / .309)


def test_izev_uses_province_model_year_and_all_observed_families(policy):
    frame = ratings([("A", 15), ("B", 25)], powertrain="bev_300_miles")
    observed = pd.concat([
        incentives([("A", 90), ("B", 10)]),
        incentives([("A", 10), ("B", 90)], province="QC"),
    ], ignore_index=True)
    observed.loc[observed.province.eq("QC"), "source_row"] += 100
    result = aggregate(frame, policy, izev=observed, regions=["ON", "QC", "AB"])
    values = result.class_audit.set_index("region")
    assert values.loc["ON", "native_class_mean"] == 16
    assert values.loc["QC", "native_class_mean"] == 24
    assert values.loc["AB", "native_class_mean"] == 20
    assert values.loc["AB", "aggregation_method"] == "izev_incentive_weighted"
    assert values.loc["AB", "pool_geography"] == "national"
    assert set(result.observations.request_year_min) == {2024}
    assert set(result.observations.year) == {2023}
    assert result.variants.groupby("region").variant_weight.sum().to_dict() == pytest.approx({"ON": 1, "QC": 1, "AB": 1})


@pytest.mark.parametrize("year", [2019, 2025])
def test_izev_boundary_years_remain_explicit_catalogue_fallback(policy, year):
    result = aggregate(ratings([("A", 15)], year=year, powertrain="bev_300_miles"), policy,
                       izev=incentives([("A", 100)], year=year))
    assert result.observations.match_status.item() == "outside_supported_program_window"
    assert result.class_audit.aggregation_method.item() == "catalogue_fallback_median_no_empirical_support"
    assert result.variants.empty


@pytest.mark.parametrize("other", ["powertrain", "rating_class"])
def test_izev_clear_family_uses_median_range_but_cannot_guess_size_class(policy, other):
    frame = ratings([("A Standard", 15), ("A Long Range", 25)], powertrain="bev_300_miles")
    frame.loc[1, other] = "bev_400_miles" if other == "powertrain" else "Mid-size"
    if other == "powertrain":
        frame.loc[1, "range_km"] = 650.
    result = aggregate(frame, policy, izev=incentives([("A", 100)]))
    if other == "powertrain":
        assert result.observations.match_status.item() == "resolved"
        assert result.observations.observed_family_range_median_km.item() == 575.
        assert result.variants.allocated_count.sum() == 100
        assert result.variants.powertrain.eq("bev_400_miles").all()
        assert set(result.matches.catalogue_powertrain) == {"bev_300_miles", "bev_400_miles"}
    else:
        assert result.observations.match_status.item() == "ambiguous_rating_class"
        assert result.variants.empty
        assert result.class_audit.aggregation_method.eq("catalogue_fallback_median_no_empirical_support").all()


def test_missing_empirical_subclass_retains_median_and_original_outer_weight(policy):
    compact = ratings([("A", 5)], rating_class="Compact")
    midsize = ratings([("B", 15)], rating_class="Mid-size")
    midsize.source_row += 100
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": cls, "aggregation_weight": weight}
        for cls, weight in [("Compact", .3), ("Mid-size", .7)]
    ])
    result = aggregate(pd.concat([compact, midsize], ignore_index=True), policy,
                       mto=stock([("A", 100)]), weights=weights)
    assert result.class_audit.set_index("rating_class").weight.to_dict() == {"Compact": .3, "Mid-size": .7}
    missing = result.coverage.set_index("rating_class").loc["Mid-size"]
    assert missing.aggregation_method == "catalogue_fallback_median_no_empirical_support"
    assert missing.used_in_aggregation
    assert missing.weight == .7
    assert missing.rating_statistic == "median"
    assert (result.class_audit.weight * result.class_audit.native_class_mean).sum() == 12


@pytest.mark.parametrize("value", [-1, .5, np.inf, np.nan])
def test_invalid_empirical_counts_fail_before_weighting(policy, value):
    source = stock([("A", value)])
    with pytest.raises(ValueError, match="counts"):
        aggregate(ratings([("A", 5)]), policy, mto=source)


def test_conflicting_variant_identity_fails_instead_of_merging_values(policy):
    source = ratings([("A", 5), ("A", 7)])
    narrower = deepcopy(policy)
    narrower["variant_identity_columns"] = ["Model"]
    with pytest.raises(ValueError, match="conflicting rating consumption"):
        aggregate(source, narrower, mto=stock([("A", 100)]))


def test_input_frames_remain_unmodified(policy):
    frame, mapped, observed = ratings([("A", 5)]), stock([("A", 100)]), incentives([("A", 100)])
    originals = [item.copy(deep=True) for item in (frame, mapped, observed)]
    aggregate(frame, policy, mto=mapped, izev=observed)
    for before, after in zip(originals, (frame, mapped, observed), strict=True):
        pd.testing.assert_frame_equal(before, after)


def test_source_canonical_alias_corrections_keep_original_duplicate_merge_audit(policy):
    mapped = stock([("Mazda3", 88_035), ("Mazda3", 38_174)], make="Mazda")
    mapped["raw_canonical_make"] = "Mazda"
    mapped["raw_canonical_model"] = ["3", "Mazda3"]
    result = aggregate(ratings([("Mazda3", 7)], make="Mazda"), policy, mto=mapped)
    assert result.observations.duplicate_identity_merged.item()
    assert result.observations.raw_model.item() == "3 | Mazda3"
    assert result.class_audit.selected_source_share.item() == 1
    assert result.class_audit.composition_source.item() == "ontario_ministry_transport_vehicle_population"
    assert result.class_audit.composition_components.item() == "A"


def test_unsupported_izev_observations_remain_auditable_without_allocations(policy):
    result = aggregate(ratings([("A", 15)], powertrain="bev_300_miles"), policy,
                       izev=incentives([("A", 30)], powertrain="fcev"))
    assert result.observations.match_status.item() == "unsupported_observed_powertrain"
    assert not result.observations.selected.any()
    assert result.variants.empty
    assert "variant_weight" in result.variants


def test_long_izev_source_row_references_are_bounded_and_fingerprinted(policy):
    observed = pd.concat([incentives([("A", 1)]) for _ in range(50)], ignore_index=True)
    observed["source_row"] = range(2, 52)
    observed["component"] = "fy2023"
    result = aggregate(ratings([("A", 15)], powertrain="bev_300_miles"), policy, izev=observed)
    row = result.observations.iloc[0]
    assert row.source_count == 50
    assert row.source_row_min == 2 and row.source_row_max == 51
    assert row.raw_observation_ids.startswith("fy2023:rows 2-51; n=50; row_identity_sha256=")
    assert len(row.raw_observation_ids) < 150
    assert result.class_audit.composition_components.item() == "fy2023"


@pytest.mark.parametrize("field", ["method", "unmatched", "ambiguous", "uncovered_cohort", "variant_allocation"])
def test_unsupported_policies_fail_instead_of_being_ignored(policy, field):
    policy[field] = "unsupported"
    with pytest.raises(ValueError, match="Unsupported empirical composition"):
        aggregate(ratings([("A", 5)]), policy, mto=stock([("A", 100)]))


def test_naming_duplicate_variant_does_not_bias_other_distinct_variant(policy):
    frame = ratings([("Mazda3", 5), ("3", 5), ("Mazda3 Sport", 9)], make="Mazda")
    result = aggregate(frame, policy, mto=stock([("Mazda3", 100)], make="Mazda"))
    assert result.class_audit.native_class_mean.item() == 7
    assert result.variants.rating_variant_id.nunique() == 2


def test_historical_suv_uses_existing_coarse_source_class_and_conserves_family_count(policy):
    frame = ratings([("A", 10), ("A AWD", 20)], year=2010, rating_class="SUV")
    small = stock([("A", 20)], year=2010, rating_class="Small SUV")
    large = stock([("A", 80)], year=2010, rating_class="Large SUV")
    large["MODEL"] = "other_source_key"
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Light Truck",
         "nrcan_vehicle_class": cls, "aggregation_weight": weight}
        for cls, weight in [("Small SUV", .3), ("Large SUV", .7)]
    ])
    result = aggregate(frame, policy, mto=pd.concat([small, large], ignore_index=True), weights=weights)
    assert len(result.observations) == 1
    assert result.observations.source_count.item() == 100
    assert result.observations.raw_nrcan_vehicle_classes.item() == "Large SUV | Small SUV"
    assert result.class_audit.rating_class.item() == "SUV"
    assert result.class_audit.native_class_mean.item() == 15
    assert result.class_audit.weight.item() == 1
    assert result.variants.allocated_count.sum() == 100


def test_historical_suv_rollup_does_not_coarsen_already_split_source_year(policy):
    frame = ratings([("A", 10)], year=2023, rating_class="Small SUV")
    result = aggregate(frame, policy, mto=stock([("A", 100)], rating_class="Large SUV"))
    assert result.observations.match_status.item() == "unmatched_ratings"
    assert result.observations.rating_class.item() == "Large SUV"


def test_unresolved_raw_identity_does_not_poison_clear_same_family_or_drop_denominator(policy):
    policy["unresolved_mto_keys"] = ["Fixture/code0"]
    policy["unresolved_mto_reason"] = "Retained source-key evidence supports incompatible model families"
    result = aggregate(ratings([("A", 7)]), policy, mto=stock([("A", 60), ("A", 40)]))
    assert len(result.observations) == 2
    assert result.observations.groupby("match_status").source_count.sum().to_dict() == {
        "resolved": 40, "unresolved_mapping_identity": 60,
    }
    assert result.class_audit.observed_source_count.item() == 100
    assert result.class_audit.selected_source_count.item() == 40
    assert result.class_audit.selected_source_share.item() == .4
    assert result.variants.allocated_count.sum() == 40
    assert result.observations.loc[result.observations.match_status.eq("unresolved_mapping_identity"),
                                   "identity_exclusion_reason"].item() == policy["unresolved_mto_reason"]


def test_izev_description_aliases_do_not_relabel_generic_mto_family(policy):
    policy["izev_aliases"] = [{"make": "MINI", "canonical_model": "Cooper SE Countryman ALL4",
                               "models": ["Countryman", "Countryman ALL4"]}]
    frame = ratings([("Countryman", 8)], make="MINI")
    phev = ratings([("Cooper SE Countryman ALL4", 4)], make="MINI", powertrain="phev_35_miles")
    phev.source_row += 100
    result = aggregate(pd.concat([frame, phev], ignore_index=True), policy,
                       mto=stock([("Countryman", 90)], make="MINI"),
                       izev=incentives([("Countryman", 10)], make="MINI", powertrain="phev"))
    names = result.observations.set_index("source_kind").family_model.to_dict()
    assert names == {"mto": "Countryman", "izev": "Cooper SE Countryman ALL4"}
    assert result.class_audit.set_index("powertrain").native_class_mean.to_dict() == {
        "gasoline": 8, "phev_35_miles": 4,
    }


def test_izev_aliases_pair_renamed_rating_editions_without_relabeling_mto(policy):
    policy["izev_aliases"] = [{"make": "Toyota", "canonical_model": "RAV4 Prime",
                               "models": ["RAV4 Plug-in Hybrid (Prime)"]}]
    frame = ratings([("RAV4 Plug-in Hybrid (Prime)", 4)], make="Toyota", powertrain="phev_50_miles")
    result = aggregate(frame, policy, izev=incentives([("RAV4 Prime", 100)], make="Toyota", powertrain="phev"))
    assert result.observations.match_status.item() == "resolved"
    assert result.class_audit.aggregation_method.item() == "izev_incentive_weighted"
    assert result.variants.Model.item() == "RAV4 Plug-in Hybrid (Prime)"


def test_unresolved_mapping_keys_require_evidence_reason(policy):
    policy["unresolved_mto_keys"] = ["Fixture/code0"]
    with pytest.raises(ValueError, match="evidence reason"):
        aggregate(ratings([("A", 5)]), policy, mto=stock([("A", 100)]))


def test_requested_provinces_do_not_change_each_others_counts_or_rating_weights(policy):
    frame = ratings([("A", 15), ("B", 25)], powertrain="bev_300_miles")
    observed = pd.concat([
        incentives([("A", 90), ("B", 10)]),
        incentives([("A", 10), ("B", 90)], province="BC"),
        incentives([("A", 300)], province="QC"),
    ], ignore_index=True)
    alone = aggregate(frame, policy, izev=observed, regions=["ON"])
    together = aggregate(frame, policy, izev=observed, regions=["ON", "BC"])
    pd.testing.assert_frame_equal(alone.class_audit.reset_index(drop=True),
                                  together.class_audit.loc[together.class_audit.region.eq("ON")].reset_index(drop=True))
    fields = ["family_make", "family_model", "source_count", "match_status", "selected", "relative_contribution"]
    pd.testing.assert_frame_equal(alone.observations[fields].reset_index(drop=True),
                                  together.observations.loc[together.observations.region.eq("ON"), fields].reset_index(drop=True))
    assert set(alone.observations.province) == {"ON"}
    assert set(together.observations.province) == {"ON", "BC"}


def test_requested_province_keeps_unsupported_year_and_type_dispositions(policy):
    observed = pd.concat([
        incentives([("A", 5)], year=2019),
        incentives([("A", 30)], powertrain="fcev"),
        incentives([("A", 100)], province="QC"),
    ], ignore_index=True)
    result = aggregate(ratings([("A", 15)], powertrain="bev_300_miles"), policy, izev=observed)
    assert result.observations.groupby("match_status").source_count.sum().to_dict() == {
        "outside_supported_program_window": 5,
        "unsupported_observed_powertrain": 30,
        "resolved": 100,
    }


def test_ambiguous_epa_powertrain_evidence_cannot_assign_mto_counts_to_gasoline(policy):
    frame = ratings([("Shared", 5), ("Clear", 10)])
    frame["ambiguous_epa_match"] = [True, False]
    original = frame.copy(deep=True)
    result = aggregate(frame, policy, mto=stock([("Shared", 100), ("Clear", 10)]))
    status = result.observations.set_index("family_model").match_status.to_dict()
    assert status == {"Clear": "resolved", "Shared": "ambiguous_powertrain_evidence"}
    assert result.class_audit.native_class_mean.item() == 10
    assert result.class_audit.observed_source_count.item() == 110
    assert result.class_audit.selected_source_count.item() == 10
    pd.testing.assert_frame_equal(frame, original)


def test_bct_uses_configured_disjoint_recipient_union_without_other_province_borrowing(policy):
    policy["province_map"] = {"BCT": ["BC", "NT", "NU", "YT"]}
    observed = pd.concat([
        incentives([("A", 30), ("B", 10)], province="BC"),
        incentives([("A", 10)], province="NT"),
        incentives([("B", 20)], province="NU"),
        incentives([("B", 5)], province="YT"),
        incentives([("A", 1000)], province="ON"),
    ], ignore_index=True)
    result = aggregate(ratings([("A", 15), ("B", 25)], powertrain="bev_300_miles"), policy,
                       izev=observed, regions=["BCT"])
    row = result.class_audit.iloc[0]
    assert row.observed_source_count == 75
    assert row.selected_source_count == 75
    assert row.empirical_families == 2
    assert row.native_class_mean == pytest.approx((40 * 15 + 35 * 25) / 75)
    assert row.composition_geography == "BC | NT | NU | YT"
    assert set(result.observations.province) == {"BC", "NT", "NU", "YT"}
    assert result.variants.allocated_count.sum() == pytest.approx(75)
    assert result.observations.relative_contribution.sum() == pytest.approx(1)


@pytest.mark.parametrize("mapping", [[], ["BC", "BC"], ["BC", ""]])
def test_recipient_province_union_must_be_explicit_nonempty_and_disjoint(policy, mapping):
    policy["province_map"] = {"BCT": mapping}
    with pytest.raises(ValueError, match="recipient-province codes"):
        aggregate(ratings([("A", 15)], powertrain="bev_300_miles"), policy,
                  izev=incentives([("A", 10)]), regions=["BCT"])


def test_purchases_and_leases_both_count_for_weights_and_pool_selection(policy):
    policy["izev_pool_selection"]["minimum_observations"] = 100
    purchased = incentives([("A", 90)])
    leased = incentives([("B", 900)])
    leased["Purchase or Lease"] = "Lease"
    national = incentives([("B", 100)], province="QC")
    result = aggregate(ratings([("A", 15), ("B", 25)], powertrain="bev_300_miles"), policy,
                       izev=pd.concat([purchased, leased, national], ignore_index=True))
    row = result.class_audit.iloc[0]
    assert row.pool_geography == "provincial"
    assert row.provincial_observation_count == 990
    assert row.pool_observation_count == 990
    assert row.observed_source_count == 990
    assert row.native_class_mean == pytest.approx((90 * 15 + 900 * 25) / 990)
    assert result.observations.transaction_eligible.all()
    assert set(result.observations.transaction_types) == {"Purchase", "Lease"}
    assert set(result.observations.province) == {"ON"}
    assert result.variants.allocated_count.sum() == 990


@pytest.mark.parametrize("count,expected_pool", [(99, "national"), (100, "provincial")])
def test_provincial_observation_threshold_boundary_is_before_matching(policy, count, expected_pool):
    policy["izev_pool_selection"]["minimum_observations"] = 100
    observed = pd.concat([incentives([("A", count)]), incentives([("B", 900)], province="QC")], ignore_index=True)
    result = aggregate(ratings([("A", 10), ("B", 30)], powertrain="bev_300_miles"), policy, izev=observed)
    row = result.class_audit.iloc[0]
    assert row.pool_geography == expected_pool
    assert row.provincial_observation_count == count
    assert row.native_class_mean == pytest.approx((count * 10 + 900 * 30) / (count + 900) if count == 99 else 10)


def test_small_provincial_class_range_counts_still_use_the_sufficient_broad_pool(policy):
    policy["izev_pool_selection"]["minimum_observations"] = 100
    frame = pd.concat([
        ratings([("Regional A", 10), ("National A", 30)], powertrain="bev_200_miles"),
        ratings([("Unobserved C", 50)], rating_class="Mid-size", powertrain="bev_200_miles"),
        ratings([("Unobserved D", 70)], powertrain="bev_300_miles"),
        ratings([("Regional B", 25), ("National B", 45)], rating_class="Mid-size", powertrain="bev_300_miles"),
    ], ignore_index=True)
    frame["source_row"] = range(2, len(frame) + 2)
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": cls, "aggregation_weight": weight}
        for cls, weight in [("Compact", .4), ("Mid-size", .6)]
    ])
    observed = pd.concat([
        incentives([("Regional A", 2), ("Regional B", 98)]),
        incentives([("National A", 1000), ("National B", 1000)], province="QC"),
    ], ignore_index=True)
    result = aggregate(frame, policy, izev=observed, weights=weights)
    audit = result.class_audit.set_index(["rating_class", "powertrain"])
    assert audit.pool_geography.eq("provincial").all()
    assert audit.provincial_observation_count.eq(100).all()
    assert audit.pool_observation_count.eq(100).all()
    assert set(result.observations.province) == {"ON"}
    for key, count, value in [(("Compact", "bev_200_miles"), 2, 10), (("Mid-size", "bev_300_miles"), 98, 25)]:
        row = audit.loc[key]
        assert row.selected_source_count == count < 100
        assert row.native_class_mean == value
        assert row.aggregation_method == "izev_incentive_weighted"
        assert row.empirical_families == 1
    assert audit.loc[("Mid-size", "bev_200_miles"), "native_class_mean"] == 50
    assert audit.loc[("Compact", "bev_300_miles"), "native_class_mean"] == 70
    assert audit.loc[("Compact", "bev_200_miles"), "weight"] == .4
    assert audit.loc[("Mid-size", "bev_200_miles"), "weight"] == .6
    assert result.observations.relative_contribution.eq(1).all()
    assert result.variants.allocated_count.sum() == 100


def test_missing_subclass_does_not_trigger_national_fill_for_sufficient_province(policy):
    policy["izev_pool_selection"]["minimum_observations"] = 100
    frame = pd.concat([ratings([("A", 15)], powertrain="bev_300_miles"),
                       ratings([("B", 25)], rating_class="Mid-size", powertrain="bev_300_miles")], ignore_index=True)
    frame.loc[1, "source_row"] = 102
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": cls, "aggregation_weight": weight}
        for cls, weight in [("Compact", .4), ("Mid-size", .6)]
    ])
    source = pd.concat([incentives([("A", 100)]), incentives([("B", 1000)], province="QC")], ignore_index=True)
    result = aggregate(frame, policy, izev=source, weights=weights)
    assert result.class_audit.rating_class.tolist() == ["Compact", "Mid-size"]
    assert result.class_audit.set_index("rating_class").weight.to_dict() == {"Compact": .4, "Mid-size": .6}
    assert result.class_audit.pool_geography.eq("provincial").all()
    assert set(result.observations.province) == {"ON"}
    assert result.coverage.set_index("rating_class").loc["Mid-size", "used_in_aggregation"]
    assert result.class_audit.set_index("rating_class").loc["Mid-size", "aggregation_method"] == "catalogue_fallback_median_no_empirical_support"


def test_sparse_province_replaces_the_whole_pool_with_national_counts(policy):
    policy["izev_pool_selection"]["minimum_observations"] = 100
    first = ratings([("A", 15)], powertrain="bev_300_miles")
    second = ratings([("B", 25)], rating_class="Mid-size", powertrain="bev_300_miles")
    second.source_row += 100
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": cls, "aggregation_weight": weight}
        for cls, weight in [("Compact", .4), ("Mid-size", .6)]
    ])
    observed = pd.concat([incentives([("A", 99)]), incentives([("B", 901)], province="QC")], ignore_index=True)
    result = aggregate(pd.concat([first, second], ignore_index=True), policy, izev=observed, weights=weights)
    assert result.class_audit.pool_geography.eq("national").all()
    assert result.class_audit.observed_source_count.eq(1000).all()
    assert result.variants.allocated_count.sum() == 1000
    assert (result.class_audit.native_class_mean * result.class_audit.weight).sum() == 21


@pytest.mark.parametrize("powertrain", ["hev", "diesel"])
def test_hev_and_diesel_use_median_of_distinct_model_variant_means(policy, powertrain):
    frame = ratings([("A", 5), ("A", 15), ("B", 20), ("C", 100)], powertrain=powertrain)
    result = aggregate(frame, policy, mto=stock([("A", 999), ("B", 1)]))
    row = result.class_audit.iloc[0]
    assert row.native_class_mean == 20
    assert row.class_mean_mj_per_vkm == 8
    assert row.rating_statistic == "median"
    assert row.aggregation_method == "catalogue_distinct_model_median"
    assert result.variants.empty
    assert not result.observations.selected.any()


def test_multi_range_family_uses_median_native_range_instead_of_default_300(policy):
    frame = ratings([("A Standard", 15), ("A Long Range", 25)], powertrain="bev_150_miles")
    frame.loc[1, "powertrain"] = "bev_200_miles"
    frame["range_km"] = [240, 350]
    result = aggregate(frame, policy, izev=incentives([("A", 100)]))
    assert set(result.class_audit.powertrain) == {"bev_150_miles", "bev_200_miles"}
    assert result.observations.match_status.item() == "resolved"
    assert result.observations.observed_family_range_median_km.item() == 295.
    assert result.observations.resolved_powertrain.item() == "bev_200_miles"
    assert result.variants.allocated_count.sum() == 100
    assert result.matches.allocated_count.sum() == 100
    assert result.matches.assigned_powertrain.eq("bev_200_miles").all()
    assert set(result.matches.powertrain) == {"bev_150_miles", "bev_200_miles"}
    assert result.class_audit.set_index("powertrain").loc["bev_200_miles", "native_class_mean"] == 20


@pytest.mark.parametrize("description", ["A Long Range AWD"])
def test_specific_trim_resolves_one_range_from_supported_rating_rows(policy, description):
    frame = ratings([("A Long Range", 15), ("A Long Range AWD", 25), ("A Standard", 20)], powertrain="bev_200_miles")
    frame.loc[1, "powertrain"] = "bev_400_miles"
    frame.loc[1, "range_km"] = 650.
    result = aggregate(frame, policy, izev=incentives([(description, 100)]))
    assert result.observations.match_status.item() == "resolved"
    assert result.observations.range_assignment.item() == "single_configuration_range"
    assert result.variants.Model.tolist() == ["A Long Range AWD"]
    assert result.variants.powertrain.eq("bev_400_miles").all()
    assert result.variants.native_consumption.item() == 25


def test_phev_range_reassignment_requires_component_evidence(policy):
    frame = ratings([("A Standard", 4), ("A Long Range", 5)], powertrain="phev_35_miles")
    frame.loc[1, "powertrain"] = "phev_50_miles"
    with pytest.raises(ValueError, match="CD/CS components"):
        aggregate(frame, policy, izev=incentives([("A", 100)], powertrain="phev"))


def test_candidate_audit_includes_unselected_and_rejected_source_rows(policy):
    frame = ratings([(f"A{number}", 5 + number) for number in range(6)])
    mapped = stock([(f"A{number}", 100 - number) for number in range(6)])
    mapped.loc[5, "match_method"] = "automatic_approved_current_candidate_approved_similarity_string_similarity"
    result = aggregate(frame, policy, mto=mapped)
    assert set(result.matches.source_row) == set(frame.source_row)
    assert not result.matches.loc[result.matches.Model.eq("A5"), "selected"].any()
    assert result.matches.loc[result.matches.Model.eq("A5"), "allocated_count"].sum() == 0
    assert result.matches.variant_weight.sum() == pytest.approx(1)


def test_size_subclass_cannot_duplicate_empirical_counts_across_ceud_classes(policy):
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": cls,
         "nrcan_vehicle_class": "Compact", "aggregation_weight": 1.}
        for cls in ["Car", "Light Truck"]
    ])
    with pytest.raises(ValueError, match="multiple CEUD"):
        aggregate(ratings([("A", 15)]), policy, mto=stock([("A", 100)]), weights=weights)


def test_transaction_weighting_rejects_missing_purchase_lease_evidence(policy):
    observed = incentives([("A", 100)]).drop(columns="Purchase or Lease")
    with pytest.raises(ValueError, match="purchase/lease column"):
        aggregate(ratings([("A", 15)], powertrain="bev_300_miles"), policy, izev=observed)


def test_generic_family_label_does_not_prove_the_exact_named_base_trim(policy):
    frame = ratings([("ID.4", 15), ("ID.4 Pro", 25), ("ID.4 AWD Pro", 20)], make="Volkswagen", powertrain="bev_200_miles")
    frame.loc[1:, "powertrain"] = "bev_300_miles"
    frame.loc[1:, "range_km"] = 500.
    result = aggregate(frame, policy, izev=incentives([("ID.4", 100)], make="Volkswagen"))
    assert result.observations.match_status.item() == "resolved"
    assert set(result.matches.Model) == {"ID.4", "ID.4 Pro", "ID.4 AWD Pro"}
    assert result.class_audit.set_index("powertrain").loc["bev_300_miles", "native_class_mean"] == 20
    assert set(result.variants.Model) == {"ID.4", "ID.4 Pro", "ID.4 AWD Pro"}
    assert result.variants.set_index("Model").family_median_weight.to_dict() == {"ID.4": 0., "ID.4 Pro": 0., "ID.4 AWD Pro": 1.}


@pytest.mark.parametrize("description,expected", [
    ("Q4 e-tron Quattro", {"Q4 50 e-tron quattro"}),
    ("Q4 e-tron", {"Q4 45 e-tron", "Q4 50 e-tron quattro"}),
    ("Q4 50 e-tron Quattro", {"Q4 50 e-tron quattro"}),
])
def test_audi_reviewed_family_pattern_preserves_explicit_numeric_trim_precision(policy, description, expected):
    frame = ratings([("Q4 45 e-tron", 15), ("Q4 50 e-tron quattro", 25),
                     ("Q4 Sportback 50 e-tron", 30)], make="Audi", powertrain="bev_300_miles")
    frame.loc[1, "powertrain"] = "bev_200_miles"
    frame.loc[1, "range_km"] = 350.
    result = aggregate(frame, policy, izev=incentives([(description, 100)], make="Audi"))
    if description == "Q4 e-tron":
        assert set(result.matches.Model) == expected
        assert result.observations.match_status.item() == "resolved"
        assert result.observations.observed_family_range_median_km.item() == 425.
        assert result.variants.powertrain.eq("bev_300_miles").all()
        assert result.variants.allocated_count.sum() == 100
    else:
        assert set(result.variants.Model) == expected
        assert not result.variants.Model.str.contains("Sportback").any()
        assert result.variants.powertrain.eq("bev_200_miles").all()


def test_exact_only_bolt_identity_translation_keeps_euv_counts_distinct(policy):
    alias = {"make": "Chevrolet", "canonical_model": "Bolt EV", "models": ["Bolt"], "exact_only": True}
    policy["izev_aliases"] = [alias]
    frame = ratings([("Bolt EV", 15), ("Bolt EUV", 25)], make="Chevrolet", powertrain="bev_300_miles")
    frame.loc[1, "powertrain"] = "bev_200_miles"
    frame.loc[1, "range_km"] = 350.
    result = aggregate(frame, policy, izev=incentives([("Bolt", 90), ("Bolt EUV", 10)], make="Chevrolet"))
    assert canonical_family("Chevrolet", "Bolt EUV", {"aliases": [alias]}) == ("Chevrolet", "Bolt EUV")
    assert result.observations.set_index("family_model").source_count.to_dict() == {"Bolt EV": 90, "Bolt EUV": 10}
    assert result.observations.match_status.eq("resolved").all()
    assert result.variants.groupby("Model").allocated_count.sum().to_dict() == {"Bolt EV": 90, "Bolt EUV": 10}
    assert result.class_audit.set_index("powertrain").native_class_mean.to_dict() == {
        "bev_300_miles": 15, "bev_200_miles": 25,
    }
    assert "bev300_default_source_count" not in result.class_audit


@pytest.mark.parametrize("supported,expected_empirical", [(29, False), (30, True), (31, True)])
def test_identity_supported_coverage_boundary_controls_class_replacement(policy, supported, expected_empirical):
    mapped = stock([("A", supported), ("B", 100 - supported)])
    mapped.loc[1, "match_method"] = "automatic_approved_current_candidate_approved_similarity_string_similarity"
    result = aggregate(ratings([("A", 5), ("B", 20), ("C", 100)]), policy, mto=mapped)
    row = result.class_audit.iloc[0]
    assert row.resolved_gasoline_count == 100
    assert row.identity_supported_count == supported
    assert row.identity_supported_resolved_share == supported / 100
    assert bool(row.identity_coverage_met) is expected_empirical
    assert row.native_class_mean == (5 if expected_empirical else 20)
    assert row.aggregation_method == ("mto_identity_supported_weighted" if expected_empirical
                                      else "catalogue_fallback_median_insufficient_identity_support")
    assert result.variants.allocated_count.sum() == (supported if expected_empirical else 0)
    weak = result.observations.loc[~result.observations.identity_supported]
    assert not weak.selected.any()
    assert weak.selection_status.eq("unsupported_mapping_identity").all()


def test_identity_support_threshold_rejects_unsupported_method_and_invalid_share(policy):
    for method, share in [("unsupported", .3), ("identity_supported_coverage", 0),
                          ("identity_supported_coverage", 1.01), ("identity_supported_coverage", True)]:
        changed = deepcopy(policy)
        changed["mto_selection"].update(method=method, minimum_resolved_count_share=share)
        with pytest.raises(ValueError, match="explicit identity-supported share"):
            aggregate(ratings([("A", 5)]), changed, mto=stock([("A", 100)]))


def test_weak_alias_counts_cannot_be_laundered_through_one_trusted_family_alias(policy):
    mapped = stock([("3", 10), ("Mazda3", 90)], make="Mazda")
    mapped.loc[1, "match_method"] = "automatic_exact_normalized_make_reviewed_canonical_label"
    result = aggregate(ratings([("Mazda3", 7)], make="Mazda"), policy, mto=mapped)
    assert len(result.observations) == 2
    assert result.observations.family_model.eq("Mazda3").all()
    assert result.observations.groupby("identity_supported").source_count.sum().to_dict() == {False: 90, True: 10}
    assert result.class_audit.identity_supported_count.item() == 10
    assert result.class_audit.resolved_gasoline_count.item() == 100
    assert not result.observations.selected.any()
    assert result.variants.empty
    assert result.class_audit.rating_statistic.item() == "median"


@pytest.mark.parametrize("route", [
    "automatic_exact_normalized_make_exact_normalized_model",
    "automatic_configured_make_alias_exact_normalized_model",
    "automatic_approved_current_candidate_approved_similarity_exact_normalized",
    "reviewed_external_model_vin_family_association",
])
def test_only_documented_positive_identity_routes_can_authorize_count_weights(policy, route):
    mapped = stock([("A", 100)])
    mapped["match_method"] = route
    result = aggregate(ratings([("A", 5)]), policy, mto=mapped)
    assert result.observations.identity_supported.item()
    assert result.observations.identity_evidence_methods.item() == route
    assert result.class_audit.aggregation_method.item() == "mto_identity_supported_weighted"


def test_model_vote_agreement_and_class_only_review_do_not_prove_identity(policy):
    mapped = stock([("A", 100)])
    mapped["match_method"] = "reviewed_external_class_configuration"
    mapped["candidate_pass_agreement"] = "agreement"
    mapped["agreed_model_candidate"] = "A"
    result = aggregate(ratings([("A", 5), ("B", 10), ("C", 100)]), policy, mto=mapped)
    assert not result.observations.identity_supported.item()
    assert not result.observations.selected.item()
    assert result.class_audit.native_class_mean.item() == 10
    assert result.class_audit.aggregation_method.item() == "catalogue_fallback_median_insufficient_identity_support"


def test_missing_crosswalk_identity_metadata_is_not_assumed_confident(policy):
    with pytest.raises(ValueError, match="match_method"):
        aggregate(ratings([("A", 5)]), policy, mto=stock([("A", 100)]).drop(columns="match_method"))


def test_resolved_gasoline_support_and_all_mapped_stock_coverage_remain_distinct(policy):
    gas = ratings([("A", 5), ("B", 10)])
    electric = ratings([("E", 20)], powertrain="bev_300_miles")
    electric.source_row += 100
    mapped = stock([("A", 60), ("B", 40), ("E", 1000)])
    mapped.loc[1, "match_method"] = "automatic_exact_normalized_make_anchored_consonant_abbreviation"
    result = aggregate(pd.concat([gas, electric], ignore_index=True), policy, mto=mapped)
    row = result.class_audit.set_index("powertrain").loc["gasoline"]
    assert row.resolved_gasoline_count == 100
    assert row.identity_supported_resolved_share == .6
    assert row.observed_source_count == 1100
    assert row.identity_supported_all_mapped_share == pytest.approx(60 / 1100)
    assert row.selected_source_count == 60


def test_identical_native_transaction_payloads_are_not_deduplicated(policy):
    first = incentives([("A", 1)])
    repeated = first.copy()
    repeated["source_row"] = 3
    result = aggregate(ratings([("A", 15)], powertrain="bev_300_miles"), policy,
                       izev=pd.concat([first, repeated], ignore_index=True))
    assert result.observations.source_count.item() == 2
    assert result.observations.source_rows.item() == 2
    assert result.variants.allocated_count.sum() == 2


def test_wholly_uncovered_cohort_uses_median_of_model_means(policy):
    frame = ratings([("A", 5), ("A", 15), ("B", 20), ("C", 100)])
    result = aggregate(frame, policy)
    assert result.class_audit.native_class_mean.item() == 20
    assert result.class_audit.rating_statistic.item() == "median"
    assert result.class_audit.aggregation_method.item() == "catalogue_fallback_median_no_empirical_support"


def test_historical_proxy_pools_all_five_source_years_and_uses_target_edition_variants(policy):
    old = ratings([("A", 10), ("A Sport", 14), ("B", 30)], year=2018, powertrain="bev_200_miles")
    own = ratings([("A", 150)], year=2023, powertrain="bev_300_miles", rating_class="Mid-size")
    later = ratings([("A", 1000)], year=2025, powertrain="bev_300_miles", rating_class="Mid-size")
    frame = pd.concat([old, own, later], ignore_index=True)
    frame["source_row"] = range(2, len(frame) + 2)
    sources = []
    for offset, year in enumerate(range(2020, 2025)):
        source = incentives([("A", offset + 1), ("B", 5 - offset), ("Future only", offset + 1)], year=year)
        source["component"] = f"source_{year}"
        if year == 2021:
            source["Purchase or Lease"] = "Lease"
        sources.append(source)
    sources.extend([incentives([("A", 10_000)], year=2019), incentives([("A", 10_000)], year=2025)])
    weights = pd.DataFrame([
        {"weight_basis": "all_vintages", "report_year": 2025, "nrcan_ceud_class": "Car",
         "nrcan_vehicle_class": cls, "aggregation_weight": weight}
        for cls, weight in [("Compact", .4), ("Mid-size", .6)]
    ])
    result = aggregate(frame, policy, izev=pd.concat(sources, ignore_index=True), weights=weights)
    audit = result.class_audit.set_index("year")
    historical = audit.loc[2018]
    assert historical.aggregation_method == "izev_historical_composition_proxy"
    assert historical.composition_basis == "historical_pooled_model_year_window"
    assert historical.source_model_year_min == 2020 and historical.source_model_year_max == 2024
    assert historical.source_model_years == "2020 | 2021 | 2022 | 2023 | 2024"
    assert historical.pool_observation_count == historical.observed_source_count == 45
    assert historical.selected_source_count == 30
    assert historical.native_class_mean == pytest.approx((15 * 12 + 15 * 30) / 30)
    historical_observations = result.observations.loc[result.observations.composition_basis.eq("historical_pooled_model_year_window")]
    assert historical_observations.groupby("family_model").source_count.sum().to_dict() == {"A": 15, "B": 15, "Future only": 15}
    assert historical_observations.source_model_years.eq("2020 | 2021 | 2022 | 2023 | 2024").all()
    assert historical_observations.empirical_observation_id.str.contains("historical_2018_2020_2024").all()
    historical_variants = result.variants.loc[result.variants.year.eq(2018)]
    assert set(historical_variants.Model) == {"A", "A Sport", "B"}
    assert historical_variants.powertrain.eq("bev_200_miles").all()
    assert historical_variants.rating_class.eq("Compact").all()
    assert historical_variants.powertrain.eq(historical_variants.catalogue_powertrain).all()
    assert historical_variants.allocated_count.sum() == 30
    assert historical_variants.variant_weight.sum() == pytest.approx(1)
    assert historical_variants.composition_components.str.contains("source_2020").all()
    assert historical_variants.composition_components.str.contains("source_2024").all()
    assert audit.loc[2023, "aggregation_method"] == "izev_incentive_weighted"
    assert audit.loc[2023, "native_class_mean"] == 150
    assert audit.loc[2023, "selected_source_count"] == 4
    assert audit.loc[2023, "rating_class"] == "Mid-size"
    assert audit.loc[2023, "composition_basis"] == "observed_model_year"
    assert audit.loc[2025, "aggregation_method"] == "catalogue_fallback_median_no_empirical_support"
    assert audit.loc[2025, "native_class_mean"] == 1000
    assert audit.loc[2025, "selected_source_count"] == 0
    future_only = historical_observations.loc[historical_observations.family_model.eq("Future only")]
    assert future_only.match_status.eq("unmatched_ratings").all()
    assert not future_only.selected.any()


def test_native_precoverage_records_cannot_enter_the_historical_window_pool(policy):
    observed = pd.concat([
        incentives([("A", 10_000)], year=2019),
        incentives([("A", 10), ("B", 90)], year=2023),
    ], ignore_index=True)
    result = aggregate(ratings([("A", 10), ("B", 30)], year=2019, powertrain="bev_200_miles"), policy, izev=observed)
    row = result.class_audit.iloc[0]
    assert row.pool_observation_count == row.observed_source_count == row.selected_source_count == 100
    assert row.native_class_mean == 28
    native_2019 = result.observations.loc[result.observations.composition_basis.eq("observed_model_year") & result.observations.year.eq(2019)]
    assert native_2019.source_count.sum() == 10_000
    assert native_2019.match_status.eq("outside_supported_program_window").all()
    assert not native_2019.selected.any()
    assert result.variants.allocated_count.sum() == 100
    assert not result.observations.duplicated(["region", "empirical_observation_id"]).any()


@pytest.mark.parametrize("window_count,geography", [(99, "national"), (100, "provincial")])
def test_historical_geography_threshold_uses_the_whole_source_window_before_matching(policy, window_count, geography):
    policy["izev_pool_selection"]["minimum_observations"] = 100
    observed = [incentives([("A", 20)], year=year) for year in range(2020, 2024)]
    observed.extend([incentives([("A", window_count - 80)], year=2024),
                     incentives([("B", 900)], year=2023, province="QC")])
    result = aggregate(ratings([("A", 10), ("B", 30)], year=2018, powertrain="bev_200_miles"), policy,
                       izev=pd.concat(observed, ignore_index=True))
    row = result.class_audit.iloc[0]
    assert row.pool_geography == geography
    assert row.provincial_observation_count == window_count
    assert row.pool_observation_count == (999 if geography == "national" else 100)
    assert row.native_class_mean == pytest.approx((99 * 10 + 900 * 30) / 999 if geography == "national" else 10)
    selected = result.variants
    assert set(selected.province) == ({"ON", "QC"} if geography == "national" else {"ON"})
    assert selected.allocated_count.sum() == row.selected_source_count


def test_historical_proxy_leaves_unrecognized_competing_families_unallocated(policy):
    old_short = ratings([("A Short", 15)], year=2017, powertrain="bev_150_miles")
    old_long = ratings([("A Long", 25)], year=2017, powertrain="bev_200_miles")
    own = ratings([("A", 50)], year=2023, powertrain="bev_300_miles")
    frame = pd.concat([old_short, old_long, own], ignore_index=True)
    frame["source_row"] = range(2, len(frame) + 2)
    result = aggregate(frame, policy, izev=incentives([("A", 100)]))
    historical = result.observations.loc[result.observations.composition_basis.eq("historical_pooled_model_year_window")]
    assert historical.match_status.eq("ambiguous_model_family").all()
    assert not historical.selected.any()
    assert historical.relative_contribution.eq(0).all()
    old_audit = result.class_audit.loc[result.class_audit.year.eq(2017)]
    assert set(old_audit.powertrain) == {"bev_150_miles", "bev_200_miles"}
    assert old_audit.aggregation_method.eq("catalogue_fallback_median_no_empirical_support").all()
    assert old_audit.set_index("powertrain").native_class_mean.to_dict() == {"bev_150_miles": 15, "bev_200_miles": 25}
    old_matches = result.matches.loc[result.matches.year.eq(2017)]
    assert old_matches.variant_weight.eq(0).all() and old_matches.allocated_count.eq(0).all()
    assert result.variants.year.eq(2023).all()
    assert result.variants.powertrain.eq("bev_300_miles").all()


def test_historical_proxy_has_target_scoped_counts_and_preserves_own_year_outputs(policy):
    own = ratings([("A", 100), ("B", 200)], powertrain="bev_300_miles")
    old = pd.concat([ratings([("A", 10), ("B", 30)], year=2018, powertrain="bev_200_miles"),
                     ratings([("A", 20), ("B", 40)], year=2019, powertrain="bev_200_miles")], ignore_index=True)
    frame = pd.concat([old, own], ignore_index=True)
    frame["source_row"] = range(2, len(frame) + 2)
    observed = incentives([("A", 30), ("B", 70)])
    combined = aggregate(frame, policy, izev=observed)
    native_only = aggregate(own, policy, izev=observed)
    own_combined = combined.class_audit.loc[combined.class_audit.year.eq(2023)].reset_index(drop=True)
    pd.testing.assert_frame_equal(own_combined, native_only.class_audit.reset_index(drop=True))
    historical = combined.variants.loc[combined.variants.year.lt(2020)]
    assert historical.groupby("year").allocated_count.sum().to_dict() == {2018: 100, 2019: 100}
    assert historical.groupby("year").variant_weight.sum().to_dict() == {2018: 1, 2019: 1}
    assert historical.empirical_observation_id.nunique() == 4
    assert combined.class_audit.set_index("year").native_class_mean.to_dict() == {2018: 24, 2019: 34, 2023: 170}


def test_historical_composition_window_has_one_configured_authority(policy):
    policy["izev_historical_composition"] = {"method": "pooled_supported_model_year_window", "model_year_min": 2022}
    with pytest.raises(ValueError, match="configured supported model-year window"):
        aggregate(ratings([("A", 15)], year=2018, powertrain="bev_200_miles"), policy)


def test_family_median_is_not_a_trim_mean_and_duplicate_rows_cannot_shift_it(policy):
    frame = ratings([("A Standard", 5), ("A Sport", 9), ("A Long Range", 100), ("B", 20)])
    original = aggregate(frame, policy, mto=stock([("A", 75), ("B", 25)]))
    repeated = pd.concat([frame, frame.iloc[[0, 0, 2, 2]]], ignore_index=True)
    repeated["source_row"] = range(2, len(repeated) + 2)
    duplicated = aggregate(repeated, policy, mto=stock([("A", 75), ("B", 25)]))
    assert original.class_audit.native_class_mean.item() == 11.75
    assert duplicated.class_audit.native_class_mean.item() == 11.75
    assert duplicated.variants.allocated_count.sum() == 100
    assert original.observations.set_index("family_model").observed_family_native_median.to_dict() == {"A": 9, "B": 20}
    assert original.variants.groupby("empirical_family_model").variant_weight.sum().to_dict() == {"A": .75, "B": .25}
    assert original.variants.representative_statistic.eq("family_median").all()


def test_public_family_labels_preserve_numeric_model_and_body_boundaries(policy):
    policy["family_matching"]["family_patterns"].extend([
        {"make": "Mazda", "pattern": r"(?i)^CX[- ]?5\b.*$", "canonical_model": "CX-5"},
        {"make": "Honda", "pattern": r"(?i)^Civic\s+(?:5Dr|Hatchback)\b.*$", "canonical_model": "Civic Hatchback"},
    ])
    assert catalogue_family("Mázda", "CX-5 Turbo AWD", policy) == ("Mazda", "CX-5")
    assert catalogue_family("Mazda", "CX-50 AWD", policy) == ("Mazda", "CX-50")
    assert catalogue_family("Honda", "Civic 5Dr", policy) == ("Honda", "Civic Hatchback")
    labelled = catalogue_family_labels(ratings([("Model 3 Standard", 10), ("Model 3 Long Range", 20)], make="Tesla"), policy)
    assert labelled.family_model.eq("Model 3").all()
    assert labelled.family_key.eq("TESLA/MODEL3").all()


def test_median_support_coefficients_split_duplicate_source_rows_and_reconstruct_total():
    frame = pd.DataFrame({"rating_variant_id": ["a", "a", "b", "c", "d"], "total": [2., 2., 4., 10., 20.],
                          "electric": [1., 1., 1.5, 7., 12.], "fuel": [1., 1., 2.5, 3., 8.]})
    weights = median_support_weights(frame, "total")
    assert weights.tolist() == [0., 0., .5, .5, 0.]
    assert (weights * frame.total).sum() == 7.
    assert (weights * frame.electric).sum() + (weights * frame.fuel).sum() == 7.
    frame.loc[frame.rating_variant_id.eq("a"), "total"] = 5.
    assert median_support_weights(frame, "total").tolist() == [.25, .25, 0., .5, 0.]
    frame["total"] = [2., 2., 2., 2., 20.]
    frame["electric"] = [.5, .5, 1., 1.5, 12.]
    frame["fuel"] = frame.total - frame.electric
    weights = median_support_weights(frame, "total")
    assert weights.tolist() == pytest.approx([1 / 6, 1 / 6, 1 / 3, 1 / 3, 0.])
    assert (weights * frame.electric).sum() + (weights * frame.fuel).sum() == pytest.approx(2.)
    distinct = frame.drop_duplicates("rating_variant_id")
    assert (median_support_weights(distinct, "total") * distinct.electric).sum() == pytest.approx((weights * frame.electric).sum())


@pytest.mark.parametrize("observed_model,expected_model", [("Model 3", "Model 3"), ("Model Y", "Model Y")])
def test_tesla_iZEV_counts_resolve_family_and_preserve_recipient_province(policy, observed_model, expected_model):
    frame = ratings([(expected_model + " Standard Range", 15), (expected_model + " Long Range AWD", 18),
                     (expected_model + " Performance AWD", 25)], make="Tesla", powertrain="bev_300_miles")
    frame["range_km"] = [350., 500., 520.]
    frame.loc[0, "powertrain"] = "bev_200_miles"
    observed = pd.concat([incentives([(observed_model, 900)], make="Tesla"),
                          incentives([(observed_model, 100)], make="Tesla", province="QC")], ignore_index=True)
    result = aggregate(frame, policy, izev=observed, regions=["ON", "QC"])
    selected = result.class_audit.loc[result.class_audit.selected_source_count.gt(0)].set_index("region")
    assert selected.selected_source_count.to_dict() == {"ON": 900., "QC": 100.}
    assert selected.native_class_mean.to_dict() == {"ON": 18., "QC": 18.}
    assert selected.powertrain.eq("bev_300_miles").all()
    assert result.variants.groupby("region").allocated_count.sum().to_dict() == {"ON": 900., "QC": 100.}


@pytest.mark.parametrize("make,observed_model,catalogue_models,canonical_model,powertrain", [
    ("Toyota", "RAV4 Prime", ["RAV4 Plug-in Hybrid (Prime)"], "RAV4 Prime", "phev_50_miles"),
    ("Hyundai", "Tucson PHEV", ["Tucson Plug-in Hybrid"], "Tucson Plug-in Hybrid", "phev_50_miles"),
    ("Hyundai", "Kona EV", ["Kona Electric Standard", "Kona Electric Long Range"], "Kona Electric", "bev_300_miles"),
])
def test_verified_electrified_family_names_use_representative_ratings(policy, make, observed_model, catalogue_models, canonical_model, powertrain):
    policy["izev_aliases"] = [{"make": make, "canonical_model": canonical_model, "models": [observed_model]}]
    policy["family_matching"]["family_patterns"].append({
        "make": make, "pattern": r"(?i)^" + (r"RAV4\s+(?:Prime|Plug-in Hybrid).*" if make == "Toyota" else
            r"Tucson\s+(?:PHEV|Plug-in Hybrid).*" if observed_model.startswith("Tucson") else r"Kona\s+(?:EV|Electric).*"),
        "canonical_model": canonical_model,
    })
    frame = ratings([(model, 10 + 10 * index) for index, model in enumerate(catalogue_models)], make=make, powertrain=powertrain)
    result = aggregate(frame, policy, izev=incentives([(observed_model, 100)], make=make, powertrain=powertrain.split("_")[0]))
    assert result.observations.match_status.item() == "resolved"
    assert result.observations.matched_family_model.item() == canonical_model
    assert result.variants.allocated_count.sum() == 100

    assert result.observations.observed_family_native_median.item() == (15 if len(catalogue_models) == 2 else 10)


def test_mto_authoritative_canonical_keys_take_priority_over_raw_codes_and_legacy_confidence(policy):
    policy["family_matching"]["family_patterns"].append({"make": "Honda", "pattern": r"(?i)^CR[- ]?V\b.*$", "canonical_model": "CR-V"})
    frame = ratings([("CR-V", 7), ("CR-V AWD", 9), ("CR-V Hybrid AWD", 2)], make="Honda")
    frame["Fuel type"] = "X"
    # Even a stale gasoline classification cannot admit an explicitly hybrid trim.
    mapped = stock([("CR-V", 100)], make="Honda")
    mapped[["MAKE", "MODEL"]] = ["HOND", "CRV"]
    mapped["raw_canonical_model"] = "unreviewed old residual label"
    mapped["match_method"] = "automatic_exact_normalized_make_reviewed_canonical_label"
    mapped["identity_authority"] = "reviewed_vehicle_size_class_map"
    mapped["identity_supported"] = True
    result = aggregate(frame, policy, mto=mapped)
    assert result.observations.mapped_canonical_model.item() == "CR-V"
    assert result.observations.identity_supported.item()
    assert result.observations.match_status.item() == "resolved"
    assert result.class_audit.native_class_mean.item() == 8.
    assert set(result.variants.Model) == {"CR-V", "CR-V AWD"}
    assert result.variants.allocated_count.sum() == 100
    mapped["identity_supported"] = False
    mapped["match_method"] = "automatic_exact_normalized_make_exact_normalized_model"
    rejected = aggregate(frame, policy, mto=mapped)
    assert not rejected.observations.identity_supported.item()
    assert rejected.variants.empty


def test_mto_cx5_is_not_cx50_and_gasoline_fuels_do_not_admit_diesel_or_e85(policy):
    policy["family_matching"]["family_patterns"].append({"make": "Mazda", "pattern": r"(?i)^CX[- ]?5\b.*$", "canonical_model": "CX-5"})
    frame = ratings([("CX-5 4WD", 7), ("CX-5 Turbo 4WD", 9), ("CX-50 4WD", 20),
                     ("CX-5 Diesel 4WD", 2), ("CX-5 E85 4WD", 3)], make="Mazda")
    frame["Fuel type"] = ["X", "Z", "X", "D", "E"]
    mapped = stock([("CX-5", 100)], make="Mazda")
    mapped[["MAKE", "MODEL"]] = ["MAZD", "CX5"]
    result = aggregate(frame, policy, mto=mapped)
    assert set(result.variants.Model) == {"CX-5 4WD", "CX-5 Turbo 4WD"}
    assert result.class_audit.native_class_mean.item() == 8.
    assert result.variants.allocated_count.sum() == 100


def test_mto_silverado_missing_trim_retains_gasoline_family_once(policy):
    frame = ratings([("Silverado", 10), ("Silverado 4WD", 14), ("Silverado", 6), ("Silverado 4WD", 17)], make="Chevrolet")
    frame["Fuel type"] = ["X", "Z", "D", "E"]
    # Fuel is part of the configured identity, as in production.
    policy["variant_identity_columns"].append("Fuel type")
    mapped = stock([("Silverado", 100)], make="Chevrolet")
    mapped[["MAKE", "MODEL"]] = ["CHEV", "SIL"]
    result = aggregate(frame, policy, mto=mapped)
    assert result.class_audit.native_class_mean.item() == 12.
    assert set(result.variants["Fuel type"]) == {"X", "Z"}
    assert result.variants.allocated_count.sum() == 100


def test_body_evidence_and_scoped_civic_uncertainty_are_not_overridden_by_aliases(policy):
    policy["family_matching"]["family_patterns"].append({"make": "Honda", "pattern": r"(?i)^Civic\s+(?:5Dr|Hatchback)\b.*$", "canonical_model": "Civic Hatchback"})
    policy["unresolved_mto_cohorts"] = [{"key": "HOND/CIV", "model_year_from": 2021, "model_year_to": 2025,
                                      "reason": "Reviewed class does not establish hatchback/sedan identity."}]
    frame = ratings([("Civic Hatchback", 7), ("Civic Sedan", 9)], make="Honda")
    mapped = stock([("Civic 5Dr", 100)], make="Honda")
    mapped[["MAKE", "MODEL"]] = ["HOND", "CIV"]
    mapped["identity_authority"], mapped["identity_supported"] = "reviewed_vehicle_size_class_map", True
    result = aggregate(frame, policy, mto=mapped)
    assert result.observations.match_status.item() == "unresolved_mapping_identity"
    assert result.variants.empty
    assert result.observations.identity_exclusion_reason.str.contains("hatchback/sedan").all()
    policy["unresolved_mto_cohorts"] = []
    result = aggregate(frame.loc[frame.Model.eq("Civic Sedan")], policy, mto=mapped)
    assert not result.observations.selected.any()
    assert result.variants.empty


def test_fuzzy_candidates_require_a_unique_structurally_compatible_family(policy):
    frame = ratings([("Thunderbrd", 5), ("Thundrebird", 20)])
    result = aggregate(frame, policy, mto=stock([("Thunderbird", 100)]))
    assert result.observations.match_status.item() == "ambiguous_model_family"
    assert result.observations.family_candidate_count.item() == 2
    assert result.observations.candidate_families.item() == "Thunderbrd | Thundrebird"
    assert result.variants.empty
    unique = aggregate(frame.iloc[[0]], policy, mto=stock([("Thunderbird", 100)]))
    assert unique.observations.match_method.item() == "fuzzy_family_candidates"
    assert unique.observations.match_status.item() == "resolved"
    assert unique.variants.allocated_count.sum() == 100


def test_explicit_drivetrain_and_engine_constraints_cannot_be_erased_by_family_matching(policy):
    frame = ratings([("A AWD", 5), ("A FWD", 10)])
    frame["Engine size (L)"] = [2., 3.]
    matched = aggregate(frame, policy, mto=stock([("A AWD 2.0L", 100)]))
    assert matched.variants.Model.tolist() == ["A AWD"]
    conflict = aggregate(frame, policy, mto=stock([("A AWD 3.0L", 100)]))
    assert conflict.observations.match_status.item() == "conflicting_or_unverified_configuration"
    assert conflict.variants.empty


def test_median_range_can_create_explicit_representative_only_cell_without_inventing_count(policy):
    frame = ratings([("A Standard", 10), ("A Long Range", 30)], powertrain="bev_150_miles")
    frame.loc[1, "powertrain"] = "bev_400_miles"
    frame["range_km"] = [240., 650.]
    result = aggregate(frame, policy, izev=incentives([("A", 123)]))
    selected = result.class_audit.loc[result.class_audit.selected_source_count.gt(0)].iloc[0]
    assert selected.powertrain == "bev_300_miles"
    assert not selected.representative_bucket_has_native_trim
    assert selected.selected_source_count == 123
    assert selected.native_class_mean == 20.
    assert selected.range_class_mean_km == 445.
    assert selected.original_weight == 1.
    assert result.variants.allocated_count.sum() == 123
    assert result.observations.source_count.sum() == 123
    assert result.matches.allocated_count.sum() == 123


@pytest.mark.parametrize("miles,expected", [(150., "bev_150_miles"), (150.0001, "bev_200_miles"),
                                           (250., "bev_200_miles"), (250.0001, "bev_300_miles")])
def test_family_median_range_uses_existing_inclusive_boundaries(policy, miles, expected):
    km = miles * conversion_factors()["length"]["mile_to_km"]
    frame = ratings([("A Standard", 10), ("A Long Range", 20)], powertrain=expected)
    frame["range_km"] = [km - 5., km + 5.]
    result = aggregate(frame, policy, izev=incentives([("A", 100)]))
    assert result.observations.resolved_powertrain.item() == expected
    assert result.variants.allocated_count.sum() == 100


def test_phev_assigned_range_uf_and_median_support_preserve_cd_cs_component_total(policy):
    frame = ratings([("A Standard", 3), ("A Long Range", 4), ("A Performance", 5)], powertrain="phev_35_miles")
    frame.loc[1:, "powertrain"] = "phev_50_miles"
    frame["range_km"] = [40., 70., 80.]
    frame["cd_electricity_kwh_per_100km"] = [10., 20., 30.]
    frame["cd_fuel_l_per_100km"] = [0., 1., .5]
    frame["cs_fuel_l_per_100km"] = [8., 4., 6.]
    frame["Fuel type 2"] = "X"
    frame["native_unit"] = "Le/100 km (CD)"
    frame["fleet_utility_factor"] = [.5, .7, .7]
    gas = conversion_factors()["fuel_hhv"]["gasoline_mj_per_litre"]
    uf = frame.fleet_utility_factor
    frame["utility_weighted_electricity_mj_per_vkm"] = uf * frame.cd_electricity_kwh_per_100km * .036
    frame["utility_weighted_fuel_mj_per_vkm"] = (uf * frame.cd_fuel_l_per_100km + (1 - uf) * frame.cs_fuel_l_per_100km) * gas / 100
    frame["consumption_mj_per_vkm"] = frame.utility_weighted_electricity_mj_per_vkm + frame.utility_weighted_fuel_mj_per_vkm
    result = aggregate(frame, policy, izev=incentives([("A", 100)], powertrain="phev"))
    selected = result.class_audit.loc[result.class_audit.selected_source_count.gt(0)].iloc[0]
    assert selected.powertrain == "phev_50_miles"
    assert result.variants.fleet_utility_factor.eq(.7).all()
    assert result.variants.utility_factor_basis.eq("assigned_family_median_range_bucket").all()
    assert result.variants.set_index("Model").family_median_weight.to_dict() == {"A Standard": 0., "A Long Range": 1., "A Performance": 0.}
    electricity = (result.variants.variant_weight * result.variants.utility_weighted_electricity_mj_per_vkm).sum()
    fuel = (result.variants.variant_weight * result.variants.utility_weighted_fuel_mj_per_vkm).sum()
    assert selected.class_mean_mj_per_vkm == pytest.approx(electricity + fuel)
    assert selected.class_mean_mj_per_vkm == pytest.approx(.7 * 20 * .036 + (.7 * 1 + .3 * 4) * gas / 100)
    assert result.variants.allocated_count.sum() == 100


@pytest.mark.parametrize("description,expected", [("Prius Prime", {"Prius Prime SE", "Prius Prime XSE"}),
                                                ("Prius Prime SE", {"Prius Prime SE"}),
                                                ("Prius Prime XSE", {"Prius Prime XSE"})])
def test_explicit_letter_trim_survives_family_reduction(policy, description, expected):
    policy["family_matching"]["family_patterns"].append({"make": "Toyota", "pattern": r"(?i)^Prius\s+Prime\b.*$", "canonical_model": "Prius Prime"})
    frame = ratings([("Prius Prime SE", 3), ("Prius Prime XSE", 5)], make="Toyota", powertrain="phev_50_miles")
    result = aggregate(frame, policy, izev=incentives([(description, 100)], make="Toyota", powertrain="phev"))
    assert result.observations.match_status.item() == "resolved"
    assert set(result.variants.Model) == expected
    assert result.variants.allocated_count.sum() == 100


def test_standard_range_plus_does_not_pool_standard_range_and_unknown_suffix_flags(policy):
    frame = ratings([("Model 3 Standard Range", 10), ("Model 3 Standard Range Plus", 20)], make="Tesla", powertrain="bev_200_miles")
    result = aggregate(frame, policy, izev=incentives([("Model 3 Standard Range Plus", 100)], make="Tesla"))
    assert result.variants.Model.tolist() == ["Model 3 Standard Range Plus"]
    assert result.variants.allocated_count.sum() == 100
    missing = aggregate(frame.iloc[[0]], policy, izev=incentives([("Model 3 Standard Range Plus", 100)], make="Tesla"))
    assert missing.observations.match_status.item() == "conflicting_or_unverified_configuration"
    assert missing.observations.configuration_status.item() == "unverified_explicit_trim_tokens"
    assert missing.variants.empty
    preferred = aggregate(ratings([("A Long Range AWD", 20)], powertrain="bev_300_miles"), policy,
                          izev=incentives([("A Long Range AWD Preferred", 100)]))
    assert preferred.observations.match_status.item() == "conflicting_or_unverified_configuration"
    assert preferred.variants.empty


@pytest.mark.parametrize("make,description,models,powertrain,expected", [
    ("BMW", "330e", [("330e Sedan", 5), ("330e xDrive Sedan", 9)], "phev_35_miles", 7),
    ("BMW", "i4", [('i4 eDrive35 Gran Coupe (18" Wheels)', 10), ('i4 eDrive40 Gran Coupe (19" Wheels)', 20),
                   ('i4 M50 Gran Coupe (20" Wheels)', 100)], "bev_300_miles", 20),
    ("Polestar", "2", [("2 Single Motor", 10), ("2 Dual Motor", 20), ("2 Dual Motor Performance Pack", 30),
                       ("2 BST Edition", 100)], "bev_300_miles", 25),
    ("VinFast", "VF8", [("VF8 ECO", 10), ("VF8 PLUS", 20), ("VF8 PLUS Performance", 100)], "bev_300_miles", 20),
    ("Fisker", "Ocean", [("Ocean Extreme/One", 10), ("Ocean Ultra", 30)], "bev_300_miles", 20),
    ("Mercedes-Benz", "EQB", [("EQB 250+", 10), ("EQB 350 4MATIC SUV", 30)], "bev_200_miles", 20),
    ("Volvo", "V60", [("V60 T8 AWD Recharge", 10), ("V60 T8 AWD Polestar Engineered", 30)], "phev_50_miles", 20),
])
def test_production_reviewed_families_resolve_unspecified_configurations_to_median(production_family_policy, make, description, models, powertrain, expected):
    frame = ratings(models, make=make, powertrain=powertrain)
    result = aggregate(frame, production_family_policy,
                       izev=incentives([(description, 100)], make=make, powertrain=powertrain.split("_")[0]))
    assert result.observations.match_status.item() == "resolved"
    assert result.observations.matched_family_model.item() == description
    assert result.observations.observed_family_native_median.item() == expected
    assert result.class_audit.loc[result.class_audit.selected_source_count.gt(0), "native_class_mean"].item() == expected
    assert set(result.variants.Model) == {model for model, _ in models}
    assert result.variants.allocated_count.sum() == 100
    assert result.variants.variant_weight.sum() == pytest.approx(1.)


@pytest.mark.parametrize("make,description,models,expected", [
    ("VinFast", "VF8 ECO", ["VF8 ECO", "VF8 PLUS", "VF8 PLUS Performance"], {"VF8 ECO"}),
    ("VinFast", "VF8 PLUS", ["VF8 ECO", "VF8 PLUS", "VF8 PLUS Performance"], {"VF8 PLUS", "VF8 PLUS Performance"}),
    ("VinFast", "VF8 PLUS Performance", ["VF8 ECO", "VF8 PLUS", "VF8 PLUS Performance"], {"VF8 PLUS Performance"}),
    ("Polestar", "2 Single Motor", ["2 Single Motor", "2 Dual Motor", "2 Dual Motor Performance Pack", "2 BST Edition"], {"2 Single Motor"}),
    ("Polestar", "2 Dual Motor", ["2 Single Motor", "2 Dual Motor", "2 Dual Motor Performance Pack", "2 BST Edition"], {"2 Dual Motor", "2 Dual Motor Performance Pack"}),
    ("Polestar", "2 Dual Motor Performance Pack", ["2 Single Motor", "2 Dual Motor", "2 Dual Motor Performance Pack", "2 BST Edition"], {"2 Dual Motor Performance Pack"}),
    ("Polestar", "2 BST Edition", ["2 Single Motor", "2 Dual Motor", "2 Dual Motor Performance Pack", "2 BST Edition"], {"2 BST Edition"}),
    ("Fisker", "Ocean Ultra", ["Ocean Extreme/One", "Ocean Ultra"], {"Ocean Ultra"}),
    ("Fisker", "Ocean Extreme", ["Ocean Extreme/One", "Ocean Ultra"], {"Ocean Extreme/One"}),
    ("Fisker", "Ocean One", ["Ocean Extreme/One", "Ocean Ultra"], {"Ocean Extreme/One"}),
    ("BMW", "i4 eDrive35", ['i4 eDrive35 Gran Coupe (18" Wheels)', 'i4 eDrive35 Gran Coupe (19" Wheels)',
                            'i4 eDrive40 Gran Coupe (18" Wheels)', 'i4 M50 Gran Coupe (19" Wheels)'],
        {'i4 eDrive35 Gran Coupe (18" Wheels)', 'i4 eDrive35 Gran Coupe (19" Wheels)'}),
    ("BMW", "i4 M50", ['i4 eDrive35 Gran Coupe (18" Wheels)', 'i4 M50 Gran Coupe (19" Wheels)',
                        'i4 M50 Gran Coupe (20" Wheels)'], {'i4 M50 Gran Coupe (19" Wheels)', 'i4 M50 Gran Coupe (20" Wheels)'}),
    ("BMW", 'i4 eDrive35 (18" Wheels)', ['i4 eDrive35 Gran Coupe (18" Wheels)', 'i4 eDrive35 Gran Coupe (19" Wheels)',
                                         'i4 M50 Gran Coupe (18" Wheels)'], {'i4 eDrive35 Gran Coupe (18" Wheels)'}),
])
def test_production_family_patterns_preserve_named_motor_trim_and_wheel_attributes(production_family_policy, make, description, models, expected):
    frame = ratings([(model, 10 + 5 * index) for index, model in enumerate(models)], make=make, powertrain="bev_300_miles")
    result = aggregate(frame, production_family_policy, izev=incentives([(description, 100)], make=make))
    assert result.observations.match_status.item() == "resolved"
    assert set(result.variants.Model) == expected
    assert result.variants.allocated_count.sum() == 100
    assert result.variants.variant_weight.sum() == pytest.approx(1.)


@pytest.mark.parametrize("description", ['i4 eDrive35 (21" Wheels)', 'i4 eDrive35 (18" Winter Wheels)', "i4 eDrive35 Ultra"])
def test_unsupported_explicit_wheel_or_trim_evidence_flags_after_family_reduction(production_family_policy, description):
    frame = ratings([('i4 eDrive35 Gran Coupe (18" Wheels)', 10), ('i4 eDrive35 Gran Coupe (19" Wheels)', 20)],
                    make="BMW", powertrain="bev_300_miles")
    result = aggregate(frame, production_family_policy, izev=incentives([(description, 100)], make="BMW"))
    assert result.observations.match_status.item() == "conflicting_or_unverified_configuration"
    assert result.observations.configuration_status.str.startswith("unverified_explicit_").all()
    assert not result.observations.selected.any()
    assert result.variants.empty


def test_production_volvo_family_is_rechecked_after_range_suffix_normalization(production_family_policy):
    models = [("V60 T8 AWD Recharge", 4), ("V60 T8 AWD Recharge Extended Range", 8)]
    frame = ratings(models, year=2022, make="Volvo", powertrain="phev_50_miles")
    labelled = catalogue_family_labels(frame, production_family_policy)
    assert labelled.family_model.eq("V60").all()
    assert labelled.family_key.eq("VOLVO/V60").all()
    result = aggregate(frame, production_family_policy,
                       izev=incentives([("V60", 100)], year=2022, make="Volvo", powertrain="phev"))
    assert result.observations.match_status.item() == "resolved"
    assert result.observations.observed_family_native_median.item() == 6.
    assert set(result.variants.Model) == {model for model, _ in models}
    assert result.variants.allocated_count.sum() == 100
    explicit = aggregate(frame, production_family_policy,
                         izev=incentives([("V60 T8 AWD Recharge Extended Range", 100)], year=2022, make="Volvo", powertrain="phev"))
    assert explicit.observations.match_status.item() == "resolved"
    assert explicit.variants.Model.tolist() == ["V60 T8 AWD Recharge Extended Range"]
    assert explicit.observations.observed_family_native_median.item() == 8.
    assert explicit.variants.allocated_count.sum() == 100


@pytest.mark.parametrize("make,year,description,models,powertrain", [
    ("BMW", 2020, "i3", ["i3 REx (120 Ah)"], "phev_50_miles"),
    ("BMW", 2021, "i3", ["i3 REx (120 Ah)"], "phev_50_miles"),
    ("BMW", 2022, "X3", ["X3 xDrive30e"], "phev_35_miles"),
    ("Volvo", 2024, "S60", ["S60 T8 AWD Recharge"], "phev_50_miles"),
    ("Volvo", 2024, "XC60", ["XC60 T8 AWD Recharge"], "phev_50_miles"),
    ("Kia", 2023, "Soul EV", ["Soul EV (120 Ah)", "Soul EV (180 Ah)"], "bev_200_miles"),
])
def test_remaining_reviewed_electrified_names_resolve_native_year_family(production_family_policy, make, year, description, models, powertrain):
    frame = ratings([(model, 10 + 10 * index) for index, model in enumerate(models)], year=year, make=make, powertrain=powertrain)
    result = aggregate(frame, production_family_policy,
                       izev=incentives([(description, 100)], year=year, make=make, powertrain=powertrain.split("_")[0]))
    assert result.observations.match_status.item() == "resolved"
    assert result.observations.matched_family_model.item() == description
    assert result.observations.observed_family_native_median.item() == (15 if len(models) == 2 else 10)
    assert set(result.variants.Model) == set(models)
    assert result.variants.allocated_count.sum() == 100
    assert result.variants.variant_weight.sum() == pytest.approx(1.)
    assert result.variants.year.eq(year).all()


@pytest.mark.parametrize("make,description,models,powertrain,expected", [
    ("BMW", "i3 REx (120 Ah)", ["i3 REx (120 Ah)", "i3 REx (94 Ah)"], "phev_50_miles", {"i3 REx (120 Ah)"}),
    ("Kia", "Soul EV (120 Ah)", ["Soul EV (120 Ah)", "Soul EV (180 Ah)"], "bev_200_miles", {"Soul EV (120 Ah)"}),
    ("Kia", "Soul EV (180 Ah)", ["Soul EV (120 Ah)", "Soul EV (180 Ah)"], "bev_200_miles", {"Soul EV (180 Ah)"}),
    ("Volvo", "S60 T8 AWD Polestar Engineered", ["S60 T8 AWD Recharge", "S60 T8 AWD Polestar Engineered"], "phev_50_miles", {"S60 T8 AWD Polestar Engineered"}),
    ("Volvo", "XC60 T8 AWD Recharge", ["XC60 T8 AWD Recharge", "XC60 T8 FWD Recharge"], "phev_50_miles", {"XC60 T8 AWD Recharge"}),
])
def test_remaining_family_patterns_preserve_native_battery_drive_and_trim_evidence(production_family_policy, make, description, models, powertrain, expected):
    frame = ratings([(model, 10 + 10 * index) for index, model in enumerate(models)], make=make, powertrain=powertrain)
    result = aggregate(frame, production_family_policy,
                       izev=incentives([(description, 100)], make=make, powertrain=powertrain.split("_")[0]))
    assert result.observations.match_status.item() == "resolved"
    assert set(result.variants.Model) == expected
    assert result.variants.allocated_count.sum() == 100


def test_bmw_x3_explicit_engine_and_propulsion_suffix_are_not_erased(production_family_policy):
    frame = ratings([("X3 xDrive30e", 5), ("X3 xDrive30e", 10)], year=2022, make="BMW", powertrain="phev_35_miles")
    frame["Engine size (L)"] = [2., 3.]
    matched = aggregate(frame, production_family_policy,
                        izev=incentives([("X3 xDrive30e 2.0L", 100)], year=2022, make="BMW", powertrain="phev"))
    assert matched.observations.match_status.item() == "resolved"
    assert matched.variants["Engine size (L)"].tolist() == [2.]
    assert matched.variants.allocated_count.sum() == 100
    for description in ("X3 xDrive30e 4.0L", "X3 xDrive30i"):
        unresolved = aggregate(frame, production_family_policy,
                               izev=incentives([(description, 100)], year=2022, make="BMW", powertrain="phev"))
        assert unresolved.observations.match_status.item() == "conflicting_or_unverified_configuration"
        assert unresolved.variants.empty


@pytest.mark.parametrize("make,description,catalogue_model,powertrain", [
    ("BMW", "i30", "i3 REx (120 Ah)", "phev_50_miles"),
    ("BMW", "X30", "X3 xDrive30e", "phev_35_miles"),
    ("Volvo", "S90", "S60 T8 AWD Recharge", "phev_50_miles"),
    ("Volvo", "XC90", "XC60 T8 AWD Recharge", "phev_50_miles"),
    ("Kia", "Soul EV (90 Ah)", "Soul EV (120 Ah)", "bev_200_miles"),
    ("Kia", "Soul EV (120 kWh)", "Soul EV (120 Ah)", "bev_200_miles"),
])
def test_remaining_family_patterns_never_override_model_number_or_battery_unit_conflicts(production_family_policy, make, description, catalogue_model, powertrain):
    frame = ratings([(catalogue_model, 10)], make=make, powertrain=powertrain)
    result = aggregate(frame, production_family_policy,
                       izev=incentives([(description, 100)], make=make, powertrain=powertrain.split("_")[0]))
    assert not result.observations.selected.any()
    assert result.observations.match_status.item() in {"unmatched_ratings", "conflicting_or_unverified_configuration"}
    assert result.variants.empty
