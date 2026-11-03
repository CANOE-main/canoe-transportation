from pathlib import Path

import pandas as pd
import pytest

from parameterization.road_aggregation import (
    apply_vehicle_mapping,
    build_rating_model_catalog,
    derive_aggregation_weights,
    derive_fleet_composition_weights,
    generate_mapping_candidates,
    latest_unresolved_key_worklist,
    load_rating_evidence,
    module_rules,
    normalize_vehicle_text,
    unresolved_mapping_reasons,
    validate_vehicle_mapping,
)
from utils import load_config_bundle, load_harmonization_rules


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_comparison.yaml"


def config():
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = module_rules(bundle)
    rating_rules = load_harmonization_rules(
        bundle,
        "nrcan_fuel_consumption_ratings",
    )["vehicle_class_harmonization"]
    return bundle, rules, rating_rules


def candidate_policy() -> dict[str, object]:
    _, rules, _ = config()
    bootstrap = rules["mapping_bootstrap"]
    return {
        "candidates_per_key": int(bootstrap["candidate_rows_per_key"]),
        "similarity_scores": {
            str(name): float(score)
            for name, score in bootstrap["candidate_similarity_scores"].items()
        },
    }


def mapping_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "entry_type": "mto_crosswalk",
        "mto_make_code": "FORD",
        "mto_model_code": "SPE",
        "model_year_from": 2015,
        "model_year_to": 2025,
        "canonical_make": "Ford",
        "canonical_model": "Escape",
        "vehicle_scope": "ldv",
        "nrcan_vehicle_class": "Sport utility vehicle: Small",
        "nlr_atb_class": "Small SUV",
        "nrcan_ceud_class": "Light Truck",
        "match_method": "human_reviewed_code_and_year_overlap",
        "mapping_status": "reviewed",
        "evidence_source": "NRCan Fuel Consumption Ratings",
        "supporting_rating_rows": 1,
        "review_notes": "fixture",
    }
    row.update(overrides)
    return row


def mapping_frame(*rows: dict[str, object]) -> pd.DataFrame:
    _, rules, _ = config()
    return pd.DataFrame(rows, columns=rules["mapping_columns"])


def test_reviewed_mapping_rejects_overlapping_model_year_ranges() -> None:
    _, rules, rating_rules = config()
    mapping = mapping_frame(
        mapping_row(model_year_from=2015, model_year_to=2020),
        mapping_row(model_year_from=2020, model_year_to=2025),
    )

    with pytest.raises(ValueError, match="overlapping model-year ranges"):
        validate_vehicle_mapping(
            mapping,
            rules=rules,
            rating_class_rules=rating_rules,
        )


def test_reviewed_mapping_rejects_competing_class_hierarchy() -> None:
    _, rules, rating_rules = config()
    mapping = mapping_frame(mapping_row(nlr_atb_class="Pickup"))

    with pytest.raises(ValueError, match="hierarchy mismatch"):
        validate_vehicle_mapping(
            mapping,
            rules=rules,
            rating_class_rules=rating_rules,
        )


@pytest.mark.parametrize("value", [2020.5, float("inf"), None])
def test_reviewed_mapping_rejects_noninteger_year_bounds(value) -> None:
    _, rules, rating_rules = config()
    with pytest.raises(ValueError, match="finite integers"):
        validate_vehicle_mapping(mapping_frame(mapping_row(model_year_from=value)),
                                 rules=rules, rating_class_rules=rating_rules)


@pytest.mark.parametrize("column", ["canonical_make", "canonical_model"])
def test_reviewed_mapping_requires_canonical_identity_for_ldv(column) -> None:
    _, rules, rating_rules = config()
    with pytest.raises(ValueError, match="nonblank canonical"):
        validate_vehicle_mapping(mapping_frame(mapping_row(**{column: " "})),
                                 rules=rules, rating_class_rules=rating_rules)


def test_candidate_generation_uses_reviewed_make_alias_without_accepting() -> None:
    current = pd.DataFrame(
        {
            "MAKE": ["TOYT"],
            "MODEL": ["RAV"],
            "MODEL_YEAR": [2008],
            "FIT_ACTIVE": [100],
        }
    )
    ratings = pd.DataFrame(
        {
            "Model year": [2020],
            "Make": ["Toyota"],
            "Model": ["RAV4"],
            "Vehicle class": ["Sport utility vehicle: Small"],
            "normalized_make": ["TOYOTA"],
            "normalized_model": ["RAV4"],
            "nlr_atb_class": ["Small SUV"],
            "nrcan_ceud_class": ["Light Truck"],
        }
    )

    candidates = generate_mapping_candidates(
        current,
        ratings,
        **candidate_policy(),
        canonical_aliases={"TOYT": "Toyota"},
    )

    assert candidates.loc[0, "canonical_model"] == "RAV4"
    assert candidates.loc[0, "candidate_rank"] == 1
    assert candidates.loc[0, "overlap_years"] == 0
    assert "mapping_status" not in candidates.columns


def test_rating_catalog_exhausts_labels_and_collapses_rav4_variants() -> None:
    _, rules, _ = config()
    ratings = pd.DataFrame(
        [
            {
                "Model year": 2025,
                "Make": "Toyota",
                "Model": model,
                "Vehicle class": vehicle_class,
                "normalized_make": "TOYOTA",
                "normalized_model": normalize_vehicle_text(model),
                "nlr_atb_class": nlr_class,
                "nrcan_ceud_class": ceud_class,
            }
            for model, vehicle_class, nlr_class, ceud_class in [
                ("RAV4", "Sport utility vehicle: Small", "Small SUV", "Light Truck"),
                ("RAV4 AWD", "Sport utility vehicle: Small", "Small SUV", "Light Truck"),
                ("RAV4 Hybrid AWD", "Sport utility vehicle: Small", "Small SUV", "Light Truck"),
                ("Corolla", "Compact", "Compact", "Car"),
                ("Corolla Cross", "Sport utility vehicle: Small", "Small SUV", "Light Truck"),
            ]
        ]
    )

    catalog = build_rating_model_catalog(
        ratings,
        mapping_columns=rules["mapping_columns"],
    )

    rav4 = catalog.loc[catalog["canonical_model"].eq("RAV4")]
    assert rav4["supporting_rating_rows"].item() == 3
    assert "Corolla Cross" in set(catalog["canonical_model"])


def test_rating_catalog_normalizes_pickup_variants_without_base_label() -> None:
    _, rules, _ = config()
    ratings = pd.DataFrame(
        [
            {
                "Model year": 2025,
                "Make": "Ford",
                "Model": model,
                "Vehicle class": "Pickup truck: Standard",
                "normalized_make": "FORD",
                "normalized_model": normalize_vehicle_text(model),
                "nlr_atb_class": "Pickup",
                "nrcan_ceud_class": "Light Truck",
            }
            for model in ["Ranger 4WD", "Ranger Raptor 4WD", "Ranger Tremor 4WD"]
        ]
    )

    catalog = build_rating_model_catalog(
        ratings,
        mapping_columns=rules["mapping_columns"],
    )

    assert catalog["canonical_model"].tolist() == ["Ranger"]
    assert catalog.loc[0, "supporting_rating_rows"] == 3


def test_rating_catalog_casefold_ties_have_deterministic_family() -> None:
    _, rules, _ = config()
    ratings = pd.DataFrame(
        [
            {
                "Model year": 2025,
                "Make": "Maserati",
                "Model": model,
                "Vehicle class": "Subcompact",
                "normalized_make": "MASERATI",
                "normalized_model": normalize_vehicle_text(model),
                "nlr_atb_class": "Compact",
                "nrcan_ceud_class": "Car",
            }
            for model in ["Granturismo", "GranTurismo"]
        ]
    )

    catalog = build_rating_model_catalog(
        ratings,
        mapping_columns=rules["mapping_columns"],
    )

    assert catalog.loc[0, "canonical_model"].casefold() == "granturismo"
    assert catalog.loc[0, "supporting_rating_rows"] == 2


def test_candidate_generation_supports_aliases_longer_than_six_characters() -> None:
    current = pd.DataFrame(
        {
            "MAKE": ["HYUN"],
            "MODEL": ["ELA"],
            "MODEL_YEAR": [2020],
            "FIT_ACTIVE": [100],
        }
    )
    ratings = pd.DataFrame(
        {
            "Model year": [2025],
            "Make": ["Hyundai"],
            "Model": ["Elantra"],
            "Vehicle class": ["Mid-size"],
            "normalized_make": ["HYUNDAI"],
            "normalized_model": ["ELANTRA"],
            "nlr_atb_class": ["Midsize"],
            "nrcan_ceud_class": ["Car"],
        }
    )

    candidates = generate_mapping_candidates(
        current,
        ratings,
        **candidate_policy(),
        canonical_aliases={"HYUN": "Hyundai"},
    )

    assert candidates.loc[0, "canonical_make"] == "Hyundai"
    assert candidates.loc[0, "candidate_status"] == "ranked_candidate"


def test_mapping_selects_make_model_key_by_model_year_range() -> None:
    mapping = mapping_frame(mapping_row(model_year_from=2020, model_year_to=2025))
    stock = pd.DataFrame(
        {
            "report_year": [2025, 2025],
            "VEHICLE_CLASS": ["PASSENGER", "PASSENGER"],
            "MAKE": ["FORD", "FORD"],
            "MODEL": ["SPE", "SPE"],
            "MODEL_YEAR": [2019, 2022],
            "FIT_ACTIVE": [10, 20],
        }
    )

    mapped = apply_vehicle_mapping(
        stock,
        mapping,
        accepted_statuses={"reviewed"},
    )

    assert len(mapped) == 2
    assert not mapped.loc[mapped["MODEL_YEAR"].eq(2019), "mapping_accepted"].item()
    assert mapped.loc[mapped["MODEL_YEAR"].eq(2022), "mapping_accepted"].item()


def test_non_ldv_mapping_is_resolved_but_not_ldv_eligible() -> None:
    mapping = mapping_frame(
        mapping_row(
            vehicle_scope="mhdv",
            nrcan_vehicle_class="",
            nlr_atb_class="",
            nrcan_ceud_class="",
        )
    )
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "VEHICLE_CLASS": ["COMMERCIAL"],
            "MAKE": ["FORD"],
            "MODEL": ["SPE"],
            "MODEL_YEAR": [2022],
            "FIT_ACTIVE": [20],
        }
    )

    mapped = apply_vehicle_mapping(stock, mapping, accepted_statuses={"reviewed"})

    assert mapped.loc[0, "mapping_outcome"] == "mapped_non_ldv"
    assert not mapped.loc[0, "mapping_accepted"]
    assert mapped.loc[0, "canonical_model"] == "Escape"
    assert pd.isna(mapped.loc[0, "nrcan_vehicle_class"])


def test_reviewed_identity_is_authoritative_independent_of_candidate_method() -> None:
    mapping = mapping_frame(mapping_row(mto_make_code="MAZD", mto_model_code="MZ3",
        canonical_make="Mazda", canonical_model="3", nrcan_vehicle_class="Compact",
        nlr_atb_class="Compact", nrcan_ceud_class="Car", match_method="old_residual_fuzzy_candidate"))
    stock = pd.DataFrame([{"report_year": 2025, "VEHICLE_CLASS": "PASSENGER", "MAKE": " mazd ",
        "MODEL": "mz3", "MODEL_YEAR": 2023, "FIT_ACTIVE": 35}])
    original = stock.copy(deep=True)

    mapped = apply_vehicle_mapping(stock, mapping, accepted_statuses={"reviewed"})

    assert len(mapped) == 1
    assert mapped.loc[0, "canonical_make"] == "Mazda"
    assert mapped.loc[0, "canonical_model"] == mapped.loc[0, "raw_canonical_model"] == "3"
    assert mapped.loc[0, "nrcan_vehicle_class"] == "Compact"
    assert mapped.loc[0, "identity_authority"] == "reviewed_vehicle_size_class_map"
    assert mapped.loc[0, "identity_supported"]
    assert mapped.loc[0, "match_method"] == "old_residual_fuzzy_candidate"
    assert mapped.loc[0, "MAKE"] == " mazd " and mapped.loc[0, "MODEL"] == "mz3"
    assert mapped.FIT_ACTIVE.sum() == 35
    pd.testing.assert_frame_equal(stock, original)


def test_reviewed_mapping_gap_does_not_borrow_an_adjacent_model_year() -> None:
    mapping = mapping_frame(*[mapping_row(mto_make_code="HOND", mto_model_code="CRV",
        canonical_make="Honda", canonical_model="CR-V", model_year_from=year, model_year_to=year)
        for year in [2023, 2025]])
    stock = pd.DataFrame([{"report_year": 2025, "VEHICLE_CLASS": "PASSENGER", "MAKE": "HOND",
        "MODEL": "CRV", "MODEL_YEAR": year, "FIT_ACTIVE": 10} for year in [2023, 2024, 2025]])

    mapped = apply_vehicle_mapping(stock, mapping, accepted_statuses={"reviewed"}).set_index("MODEL_YEAR")

    assert mapped.mapping_accepted.tolist() == [True, False, True]
    assert mapped.loc[2024, "identity_authority"] == ""
    assert not mapped.loc[2024, "identity_supported"]
    assert pd.isna(mapped.loc[2024, "canonical_model"])
    assert mapped.FIT_ACTIVE.sum() == 30


def test_runtime_mapping_preserves_reviewed_labels_without_residual_evidence(tmp_path, monkeypatch) -> None:
    from parameterization import road_aggregation as module

    bundle, rules, rating_rules = config()
    source = tmp_path / "source"
    source.mkdir()
    stock_file = source / "current.csv"
    pd.DataFrame([{"report_year": 2025, "VEHICLE_CLASS": "PASSENGER", "MAKE": "MAZD",
        "MODEL": "MZ3", "MODEL_YEAR": 2023, "FIT_ACTIVE": 20}]).to_csv(stock_file, index=False)
    mapping_path = tmp_path / "vehicle_size_class_map.csv"
    mapping_frame(mapping_row(mto_make_code="MAZD", mto_model_code="MZ3", canonical_make="Mazda",
        canonical_model="3", nrcan_vehicle_class="Compact", nlr_atb_class="Compact", nrcan_ceud_class="Car")).to_csv(mapping_path, index=False)
    routes = {"ontario_vehicle_population": source, "road_aggregation": tmp_path / "processed",
              "vehicle_mapping_review": tmp_path / "review"}
    monkeypatch.setattr(module, "load_config_bundle", lambda path: bundle)
    monkeypatch.setattr(module, "module_rules", lambda actual: rules)
    monkeypatch.setattr(module, "resolve_artifact_path", lambda actual, key: routes[key])
    monkeypatch.setattr(module, "resolve_parameter_path", lambda actual, filename: mapping_path)
    monkeypatch.setattr(module, "load_wards_legacy_comparison", lambda actual: pd.DataFrame())
    def harmonization(actual, name):
        if name == module.ONTARIO_RULE_KEY:
            return {"current_stock_file": stock_file.name}
        if name == module.RATINGS_RULE_KEY:
            return {"vehicle_class_harmonization": rating_rules}
        raise AssertionError("Runtime MTO identities must not be rewritten by rating aliases")
    monkeypatch.setattr(module, "load_harmonization_rules", harmonization)
    def reject_residual(*args, **kwargs):
        raise AssertionError("Runtime mapping must not consult residual evidence")
    monkeypatch.setattr(module, "resolve_input_path", reject_residual)

    output = module.build_road_aggregation_artifacts("fixture.yaml")

    mapped = pd.read_csv(output / rules["mapped_current_stock_file"], dtype={"canonical_model": str})
    assert mapped.canonical_model.tolist() == ["3"]
    assert mapped.identity_authority.tolist() == ["reviewed_vehicle_size_class_map"]
    assert mapped.FIT_ACTIVE.sum() == 20


def test_ontario_weights_sum_within_ceud_class_and_vintage() -> None:
    mapped = pd.DataFrame(
        {
            "report_year": [2025, 2025, 2025],
            "MODEL_YEAR": [2022, 2022, 2022],
            "FIT_ACTIVE": [30, 70, 50],
            "mapping_accepted": [True, True, True],
            "nrcan_ceud_class": ["Car", "Car", "Light Truck"],
            "nrcan_vehicle_class": ["Compact", "Mid-size", "Pickup truck: Small"],
            "nlr_atb_class": ["Compact", "Midsize", "Pickup"],
        }
    )

    _, rules, _ = config()
    nrcan, nlr = derive_aggregation_weights(
        mapped,
        vintage_bin_years=int(rules["vintage_bin_years"]),
        vintage_alignment=str(rules["vintage_alignment"]),
    )

    annual = nrcan.loc[nrcan["weight_basis"].eq("model_year")]
    assert annual.groupby("nrcan_ceud_class")["aggregation_weight"].sum().to_dict() == (
        pytest.approx({"Car": 1.0, "Light Truck": 1.0})
    )
    assert nlr.loc[
        nlr["nrcan_ceud_class"].eq("Car")
        & nlr["weight_basis"].eq("model_year"),
        "aggregation_weight",
    ].tolist() == pytest.approx([0.3, 0.7])
    five_year = nlr.loc[nlr["weight_basis"].eq("five_year_vintage")]
    assert set(five_year["vintage"].dropna().astype(int)) == {2025}
    assert five_year.groupby("nrcan_ceud_class")[
        "aggregation_weight"
    ].sum().to_dict() == pytest.approx({"Car": 1.0, "Light Truck": 1.0})


def test_fleet_composition_weights_use_latest_snapshot_and_age_grain() -> None:
    mapped = pd.DataFrame(
        {
            "report_year": [2024, 2025, 2025],
            "VEHICLE_CLASS": ["PASSENGER", "PASSENGER", "PASSENGER"],
            "MODEL_YEAR": [2020, 2020, 2022],
            "FIT_ACTIVE": [999, 30, 70],
            "mapping_accepted": [True, True, True],
            "nrcan_ceud_class": ["Car", "Car", "Car"],
            "nrcan_vehicle_class": ["Compact", "Compact", "Mid-size"],
            "nlr_atb_class": ["Compact", "Compact", "Midsize"],
        }
    )

    _, rules, _ = config()
    weights = derive_fleet_composition_weights(
        mapped,
        minimum_model_year=int(rules["mapping_bootstrap"]["minimum_model_year"]),
    )

    assert set(weights["report_year"]) == {2025}
    assert set(weights["age"]) == {3, 5}
    assert weights["fit_active_stock"].sum() == 100
    assert weights["share_of_mapped_fleet"].sum() == pytest.approx(1.0)
    assert weights.groupby(["vehicle_class", "nrcan_ceud_class"])[
        "age_share_within_ceud_class"
    ].sum().item() == pytest.approx(1.0)


def test_repository_ratings_load_with_existing_class_harmonization() -> None:
    bundle, rules, _ = config()

    evidence = load_rating_evidence(bundle, rules=rules)

    assert not evidence.empty
    assert set(evidence["nlr_atb_class"]) <= {
        "Compact",
        "Midsize",
        "Small SUV",
        "Midsize SUV",
        "Pickup",
    }


def test_unresolved_reason_summary_has_absolute_and_relative_stock() -> None:
    mapped = pd.DataFrame(
        {
            "report_year": [2025] * 5,
            "VEHICLE_CLASS": ["PASSENGER"] * 5,
            "MAKE": ["FORD", "FORD", "****", "NOPE", "TOYT"],
            "MODEL": ["SPE", "SPE", "UNK", "ZZZ", "COR"],
            "MODEL_YEAR": [2026, 2014, 2020, 2020, 2024],
            "FIT_ACTIVE": [10, 20, 30, 40, 50],
            "mapping_accepted": [False] * 5,
            "mapping_outcome": ["unmapped"] * 5,
        }
    )
    candidates = pd.DataFrame(
        [
            {
                "mto_make_code": make,
                "mto_model_code": model,
                "candidate_rank": rank,
                "candidate_status": status,
                "canonical_make": canonical_make,
                "canonical_model": canonical_model,
                "nrcan_vehicle_class": vehicle_class,
                "nlr_atb_class": nlr_class,
                "match_method": method,
                "model_similarity": similarity,
                "rating_model_year_from": year_from,
                "rating_model_year_to": year_to,
                "overlap_years": overlap,
            }
            for (
                make,
                model,
                rank,
                status,
                canonical_make,
                canonical_model,
                vehicle_class,
                nlr_class,
                method,
                similarity,
                year_from,
                year_to,
                overlap,
            ) in [
                (
                    "FORD",
                    "SPE",
                    1,
                    "ranked_candidate",
                    "Ford",
                    "Escape",
                    "Sport utility vehicle: Small",
                    "Small SUV",
                    "normalized_prefix",
                    0.95,
                    2015,
                    2025,
                    11,
                ),
                (
                    "****",
                    "UNK",
                    pd.NA,
                    "no_make_prefix_match",
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    0,
                ),
                (
                    "NOPE",
                    "ZZZ",
                    pd.NA,
                    "no_make_prefix_match",
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    0,
                ),
                (
                    "TOYT",
                    "COR",
                    1,
                    "ambiguous_top_score",
                    "Toyota",
                    "Corolla",
                    "Compact",
                    "Compact",
                    "normalized_prefix",
                    0.95,
                    2015,
                    2025,
                    11,
                ),
            ]
        ]
    )
    mapping = mapping_frame(mapping_row())

    detail, summary = unresolved_mapping_reasons(mapped, candidates, mapping)

    assert detail.set_index(["MAKE", "MODEL_YEAR"])[
        "unresolved_reason"
    ].to_dict() == {
        ("FORD", 2026): "unreviewed_high_confidence_candidate",
        ("FORD", 2014): "unreviewed_high_confidence_candidate",
        ("****", 2020): "suppressed_or_unknown_code",
        ("NOPE", 2020): "no_normalized_make_agreement",
        ("TOYT", 2024): "ambiguous_top_candidate",
    }
    assert summary["unresolved_rows"].sum() == 5
    assert summary["unresolved_fit_active_stock"].sum() == 150
    assert summary.groupby("VEHICLE_CLASS")[
        "share_of_unresolved_rows"
    ].sum().to_dict() == pytest.approx({"PASSENGER": 1.0})
    assert summary.groupby("VEHICLE_CLASS")[
        "share_of_unresolved_stock"
    ].sum().to_dict() == pytest.approx({"PASSENGER": 1.0})

    worklist = latest_unresolved_key_worklist(detail)
    assert set(worklist["MAKE"]) == {"FORD", "NOPE", "TOYT"}
    assert "****" not in set(worklist["MAKE"])
    assert worklist.iloc[0]["fit_active_stock"] >= worklist.iloc[-1][
        "fit_active_stock"
    ]
