from utils.vehicle_labels import (
    candidate_matches_any,
    reconcile_candidate_passes,
    vehicle_families_equivalent,
)


def test_candidate_agreement_is_case_insensitive_and_family_level() -> None:
    assert reconcile_candidate_passes("550", "550i").status == "agreement"
    assert reconcile_candidate_passes("ELANTRA", "Elantra").status == "agreement"
    assert vehicle_families_equivalent("C-Class", "C300") is False


def test_slash_candidate_selects_the_repeated_family() -> None:
    result = reconcile_candidate_passes("J-Car/Elantra", "Elantra")

    assert result.status == "agreement"
    assert result.agreed_candidate == "Elantra"


def test_disagreement_candidate_can_match_either_pass() -> None:
    result = reconcile_candidate_passes("Cruze", "Cobalt")

    assert result.status == "disagreement"
    assert candidate_matches_any("Cruze Sedan", "Cruze", "Cobalt")
    assert not candidate_matches_any("Malibu", "Cruze", "Cobalt")
