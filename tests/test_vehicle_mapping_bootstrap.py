from pathlib import Path

import pandas as pd
import pytest

import parameterization.road_aggregation as road_aggregation
from parameterization.vehicle_mapping_bootstrap import (
    apply_reviewed_class_overrides,
    automatically_supported_years,
    collapse_supported_years,
    fill_stable_family_years,
    reviewed_strong_candidate_years,
    reviewed_crosswalk_seed,
)
from utils import (
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
    resolve_parameter_path,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


def rating_row(
    *,
    make: str,
    model: str,
    vehicle_class: str,
    nlr_class: str,
    ceud_class: str,
    year: int = 2020,
) -> dict[str, object]:
    return {
        "Model year": year,
        "Make": make,
        "Model": model,
        "Vehicle class": vehicle_class,
        "normalized_make": road_aggregation.normalize_vehicle_text(make),
        "normalized_model": road_aggregation.normalize_vehicle_text(model),
        "nlr_atb_class": nlr_class,
        "nrcan_ceud_class": ceud_class,
    }


def test_reviewed_crosswalk_seed_preserves_manual_and_automatic_rows() -> None:
    reviewed = pd.DataFrame(
        {
            "entry_type": ["ratings_catalog", "mto_crosswalk", "mto_crosswalk"],
            "mto_make_code": ["", "FORD", "HOND"],
            "mto_model_code": ["", "F15", "CRV"],
            "match_method": [
                "ratings_family_token_prefix",
                "manual_review",
                "automatic_configured_make_alias_exact_normalized_model",
            ],
        }
    )

    seed = reviewed_crosswalk_seed(reviewed, policy="preserve_all")

    assert list(seed["mto_model_code"]) == ["F15", "CRV"]
    with pytest.raises(ValueError, match="Unsupported reviewed crosswalk policy"):
        reviewed_crosswalk_seed(reviewed, policy="regenerate_automatic")


def test_bootstrap_accepts_exact_and_prefix_alias_evidence() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025, 2025],
            "MAKE": ["HOND", "TOYT"],
            "MODEL": ["CRV", "COR"],
            "MODEL_YEAR": [2020, 2020],
            "FIT_ACTIVE": [100, 200],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Honda",
                model="CR-V",
                vehicle_class="Sport utility vehicle: Small",
                nlr_class="Small SUV",
                ceud_class="Light Truck",
            ),
            rating_row(
                make="Toyota",
                model="Corolla",
                vehicle_class="Compact",
                nlr_class="Compact",
                ceud_class="Car",
            ),
        ]
    )

    supported = automatically_supported_years(
        stock,
        ratings,
        canonical_aliases={"TOYT": "Toyota"},
    )

    assert supported.set_index("mto_model_code")["model_match_method"].to_dict() == {
        "COR": "normalized_model_prefix",
        "CRV": "exact_normalized_model",
    }
    assert supported["fit_active_stock"].sum() == 300


def test_bootstrap_treats_ratings_model_year_as_provenance_only() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["HOND"],
            "MODEL": ["CRV"],
            "MODEL_YEAR": [2008],
            "FIT_ACTIVE": [100],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Honda",
                model="CR-V",
                vehicle_class="Sport utility vehicle: Small",
                nlr_class="Small SUV",
                ceud_class="Light Truck",
                year=2025,
            )
        ]
    )

    supported = automatically_supported_years(stock, ratings)

    assert supported.loc[0, "model_year"] == 2008
    assert supported.loc[0, "canonical_model"] == "CR-V"


def test_bootstrap_accepts_unique_anchored_consonant_abbreviation() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["FORD"],
            "MODEL": ["RNG"],
            "MODEL_YEAR": [2020],
            "FIT_ACTIVE": [100],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Ford",
                model="Ranger 4WD",
                vehicle_class="Pickup truck: Small",
                nlr_class="Pickup",
                ceud_class="Light Truck",
            )
        ]
    )

    supported = automatically_supported_years(stock, ratings)

    assert supported.loc[0, "canonical_model"] == "Ranger"
    assert (
        supported.loc[0, "model_match_method"]
        == "anchored_consonant_abbreviation"
    )


def test_bootstrap_rejects_arbitrary_ordered_subsequence() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["FORD"],
            "MODEL": ["TST"],
            "MODEL_YEAR": [2020],
            "FIT_ACTIVE": [100],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Ford",
                model=model,
                vehicle_class=vehicle_class,
                nlr_class=nlr_class,
                ceud_class=ceud_class,
            )
            for model, vehicle_class, nlr_class, ceud_class in [
                ("Fiesta ST", "Compact", "Compact", "Car"),
                (
                    "Transit Connect",
                    "Special purpose vehicle",
                    "Midsize SUV",
                    "Light Truck",
                ),
            ]
        ]
    )

    supported = automatically_supported_years(stock, ratings)

    assert supported.empty


def test_bootstrap_rejects_crg_as_cmax_energi() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["FORD"],
            "MODEL": ["CRG"],
            "MODEL_YEAR": [2025],
            "FIT_ACTIVE": [7_049],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Ford",
                model="C-MAX Energi",
                vehicle_class="Mid-size",
                nlr_class="Midsize",
                ceud_class="Car",
            )
        ]
    )

    assert automatically_supported_years(stock, ratings).empty


def test_bootstrap_leaves_competing_nrcan_classes_unresolved() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["VOLK"],
            "MODEL": ["GLF"],
            "MODEL_YEAR": [2020],
            "FIT_ACTIVE": [200],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Volkswagen",
                model="Golf",
                vehicle_class="Compact",
                nlr_class="Compact",
                ceud_class="Car",
            ),
            rating_row(
                make="Volkswagen",
                model="Golf",
                vehicle_class="Station wagon: Small",
                nlr_class="Midsize",
                ceud_class="Car",
            ),
        ]
    )

    supported = automatically_supported_years(stock, ratings)

    assert supported.empty


def test_bootstrap_prefers_exact_mto_year_over_modal_source_label() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["NISS"],
            "MODEL": ["FRT"],
            "MODEL_YEAR": [2020],
            "FIT_ACTIVE": [200],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Nissan",
                model="Frontier",
                vehicle_class=vehicle_class,
                nlr_class="Pickup",
                ceud_class="Light Truck",
                year=year,
            )
            for year, vehicle_class in [
                (2020, "Pickup truck: Small"),
                (2021, "Pickup truck: Standard"),
                (2022, "Pickup truck: Standard"),
            ]
        ]
    )

    supported = automatically_supported_years(stock, ratings)

    assert supported.loc[0, "canonical_model"] == "Frontier"
    assert supported.loc[0, "nrcan_vehicle_class"] == "Pickup truck: Small"
    assert supported.loc[0, "year_resolution"] == "exact_mto_model_year"


def test_bootstrap_preserves_vintage_gaps_as_separate_ranges() -> None:
    supported = pd.DataFrame(
        {
            "mto_make_code": ["TOYT", "TOYT", "TOYT"],
            "mto_model_code": ["COR", "COR", "COR"],
            "model_year": [2018, 2019, 2021],
            "fit_active_stock": [10.0, 20.0, 30.0],
            "canonical_make": ["Toyota", "Toyota", "Toyota"],
            "canonical_model": ["Corolla", "Corolla", "Corolla"],
            "nrcan_vehicle_class": ["Compact", "Compact", "Compact"],
            "nlr_atb_class": ["Compact", "Compact", "Compact"],
            "nrcan_ceud_class": ["Car", "Car", "Car"],
            "make_match_method": [
                "normalized_make_prefix",
                "normalized_make_prefix",
                "normalized_make_prefix",
            ],
            "model_match_method": [
                "normalized_model_prefix",
                "normalized_model_prefix",
                "normalized_model_prefix",
            ],
            "supporting_rating_rows": [1, 1, 1],
            "supporting_model_labels": ["Corolla", "Corolla", "Corolla"],
        }
    )

    mapping = collapse_supported_years(supported)

    assert mapping[
        ["model_year_from", "model_year_to"]
    ].to_records(index=False).tolist() == [(2018, 2019), (2021, 2021)]
    assert set(mapping["mapping_status"]) == {"reviewed"}
    assert set(mapping["entry_type"]) == {"mto_crosswalk"}
    assert set(mapping["evidence_source"]) == {
        "nrcan_fuel_consumption_ratings; "
        "Ontario Vehicle Population Report A historical fit-active stock"
    }


def test_stable_canonical_family_fills_internal_vintage_gaps() -> None:
    supported = pd.DataFrame(
        {
            "mto_make_code": ["TOYT", "TOYT", "TOYT"],
            "mto_model_code": ["COR", "COR", "COR"],
            "model_year": [2003, 2008, 2020],
            "fit_active_stock": [10.0, 20.0, 10_000.0],
            "canonical_make": ["Toyota"] * 3,
            "canonical_model": ["Corolla"] * 3,
            "nrcan_vehicle_class": ["Compact", "Mid-size", "Compact"],
            "nlr_atb_class": ["Compact", "Midsize", "Compact"],
            "nrcan_ceud_class": ["Car"] * 3,
            "make_match_method": ["configured_make_alias"] * 3,
            "model_match_method": ["reviewed_canonical_label"] * 3,
            "supporting_rating_rows": [1] * 3,
            "supporting_model_labels": ["Corolla"] * 3,
            "supporting_evidence_sources": [
                "nrcan_fuel_consumption_ratings"
            ]
            * 3,
            "year_resolution": ["exact_mto_model_year"] * 3,
            "first_report_year": [2014] * 3,
            "last_report_year": [2025] * 3,
            "edition_count": [3] * 3,
        }
    )

    filled = fill_stable_family_years(
        supported,
        hierarchy_dominance_share=0.95,
    )
    mapping = collapse_supported_years(filled)

    assert mapping[["model_year_from", "model_year_to"]].to_records(
        index=False
    ).tolist() == [(2003, 2020)]
    assert mapping.loc[0, "nlr_atb_class"] == "Compact"


def test_reviewed_strong_candidates_become_supported_years() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["GMC"],
            "MODEL": ["SIE"],
            "MODEL_YEAR": [1998],
            "FIT_ACTIVE": [0],
        }
    )
    candidates = pd.DataFrame(
        {
            "mto_make_code": ["GMC"],
            "mto_model_code": ["SIE"],
            "candidate_rank": [1],
            "candidate_status": ["ranked_candidate"],
            "match_method": ["normalized_prefix"],
            "canonical_make": ["GMC"],
            "canonical_model": ["Sierra"],
            "nrcan_vehicle_class": ["Pickup truck: Standard"],
            "nlr_atb_class": ["Pickup"],
            "nrcan_ceud_class": ["Light Truck"],
            "rating_rows": [5],
            "rating_model_labels": ["Sierra"],
            "evidence_source": ["fueleconomy_gov_vehicle_data"],
        }
    )

    supported = reviewed_strong_candidate_years(stock, candidates)

    assert supported.loc[0, "model_year"] == 1998
    assert supported.loc[0, "fit_active_stock"] == 0
    assert supported.loc[0, "canonical_model"] == "Sierra"
    assert supported.loc[0, "model_match_method"] == "reviewed_canonical_label"


def test_reviewed_class_override_replaces_ambiguous_candidate_hierarchy() -> None:
    supported = pd.DataFrame(
        {
            "mto_make_code": ["FORD"],
            "mto_model_code": ["F/L"],
            "canonical_make": ["Ford"],
            "canonical_model": ["Bronco"],
            "nrcan_vehicle_class": ["Sport utility vehicle: Standard"],
            "nlr_atb_class": ["Midsize SUV"],
            "nrcan_ceud_class": ["Light Truck"],
        }
    )

    overridden = apply_reviewed_class_overrides(
        supported,
        {
            "FORD/F/L": {
                "canonical_model": "F-Series",
                "nrcan_vehicle_class": "Pickup truck: Standard",
                "nlr_atb_class": "Pickup",
            }
        },
    )

    assert overridden.loc[0, "canonical_model"] == "F-Series"
    assert overridden.loc[0, "nrcan_vehicle_class"] == "Pickup truck: Standard"
    assert overridden.loc[0, "nlr_atb_class"] == "Pickup"


def test_bootstrap_pools_historical_editions_by_make_model_vintage() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2018, 2019],
            "MAKE": ["LEXS", "LEXS"],
            "MODEL": ["RX3", "RX3"],
            "MODEL_YEAR": [2015, 2015],
            "FIT_ACTIVE": [100, 90],
        }
    )
    ratings = pd.DataFrame(
        [
            rating_row(
                make="Lexus",
                model="RX 350 AWD",
                vehicle_class="Sport utility vehicle: Small",
                nlr_class="Small SUV",
                ceud_class="Light Truck",
                year=2015,
            )
        ]
    )

    supported = automatically_supported_years(
        stock,
        ratings,
        canonical_aliases={"LEXS": "Lexus"},
    )

    assert len(supported) == 1
    assert supported.loc[0, "fit_active_stock"] == 190
    assert supported.loc[0, "first_report_year"] == 2018
    assert supported.loc[0, "last_report_year"] == 2019
    assert supported.loc[0, "edition_count"] == 2


def test_bootstrap_accepts_short_alphanumeric_model_with_mto_suffix() -> None:
    stock = pd.DataFrame(
        {
            "report_year": [2025],
            "MAKE": ["AUDI"],
            "MODEL": ["A42"],
            "MODEL_YEAR": [2011],
            "FIT_ACTIVE": [734],
        }
    )
    ratings = pd.DataFrame(
        {
            "Model year": [2025],
            "Make": ["Audi"],
            "Model": ["A4"],
            "Vehicle class": ["Compact"],
            "nlr_atb_class": ["Compact"],
            "nrcan_ceud_class": ["Car"],
        }
    )

    supported = automatically_supported_years(
        stock,
        ratings,
        minimum_model_prefix_length=3,
    )

    assert supported.loc[0, "canonical_model"] == "A4"
    assert (
        supported.loc[0, "model_match_method"]
        == "canonical_model_plus_mto_suffix"
    )


def test_runtime_reads_but_does_not_overwrite_reviewed_mapping(
    monkeypatch,
) -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = road_aggregation.module_rules(bundle)
    mapping_path = resolve_parameter_path(
        bundle,
        rules["vehicle_size_class_map_file"],
    )
    before = mapping_path.read_bytes()
    written_paths: list[Path] = []

    def capture_write(_frame: pd.DataFrame, path: Path) -> None:
        written_paths.append(path.resolve())

    monkeypatch.setattr(
        road_aggregation,
        "write_dataframe_atomic",
        capture_write,
    )

    road_aggregation.build_road_aggregation_artifacts(SCENARIO)

    assert mapping_path.read_bytes() == before
    assert mapping_path.resolve() not in written_paths
    assert written_paths


def test_repository_mapping_has_material_scale_and_all_ldv_classes() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = road_aggregation.module_rules(bundle)
    rating_rules = load_harmonization_rules(
        bundle,
        "nrcan_fuel_consumption_ratings",
    )["vehicle_class_harmonization"]
    mapping_path = resolve_parameter_path(
        bundle,
        rules["vehicle_size_class_map_file"],
    )
    mapping = pd.read_csv(mapping_path, dtype=str, keep_default_na=False)
    mapping = road_aggregation.validate_vehicle_mapping(
        mapping,
        rules=rules,
        rating_class_rules=rating_rules,
    )

    crosswalk = mapping.loc[mapping["entry_type"].eq("mto_crosswalk")]
    assert set(mapping["entry_type"]) == {"mto_crosswalk"}
    assert len(crosswalk) >= 600
    ratings = road_aggregation.load_rating_evidence(bundle, rules=rules)
    expected_catalog = road_aggregation.build_rating_model_catalog(
        ratings,
        mapping_columns=rules["mapping_columns"],
    )
    assert len(expected_catalog) >= 500
    assert expected_catalog["supporting_rating_rows"].sum() == len(ratings)
    assert crosswalk["mto_make_code"].nunique() >= 20
    assert set(crosswalk["nlr_atb_class"]) == {
        "Compact",
        "Midsize",
        "Small SUV",
        "Midsize SUV",
        "Pickup",
    }
    assert not (
        crosswalk["mto_make_code"].eq("FORD")
        & crosswalk["mto_model_code"].eq("CRG")
    ).any()
    ford_gt_codes = crosswalk.loc[
        crosswalk["mto_make_code"].eq("FORD")
        & crosswalk["mto_model_code"].isin(["MGT", "SGT"])
    ]
    assert set(ford_gt_codes["canonical_model"]) == {"Mustang"}
    assert set(ford_gt_codes["nlr_atb_class"]) == {"Compact"}
    assert not ford_gt_codes["canonical_model"].eq("GT").any()
    assert not crosswalk["match_method"].str.contains(
        "ordered_model_abbreviation",
        regex=False,
    ).any()
    assert crosswalk["match_method"].str.startswith("automatic_").sum() >= 600

    ontario_rules = load_harmonization_rules(
        bundle,
        "ontario_vehicle_population",
    )
    current_stock = pd.read_csv(
        resolve_input_path(
            bundle,
            "interim",
            ontario_rules["interim_subdir"],
            ontario_rules["current_stock_file"],
        ),
        low_memory=False,
    )
    mapped = road_aggregation.apply_vehicle_mapping(
        current_stock,
        mapping,
        accepted_statuses={"reviewed"},
    )
    coverage = road_aggregation.mapping_coverage(mapped).set_index(
        ["vehicle_class", "measure"]
    )

    assert coverage.loc[("ALL", "fit_active_stock"), "coverage"] >= 0.40
