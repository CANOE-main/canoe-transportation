"""A run-selection annotation must not invalidate unchanged external-model inputs."""
import hashlib

import pytest

from fetching.greet_vehicle_cycle import SOURCE, _atb_replay_digest, _canonical_source_key


RETAINED = b'trajectory,value,is_default_trajectory\r\nMid,42.5,False\r\nConservative,39.0,True\r\n'
REPLAYED = b'trajectory,value,is_default_trajectory\r\nMid,42.5,True\r\nConservative,39.0,False\r\n'
TRAJECTORIES = ["Mid", "Conservative"]


def test_atb_marker_only_replay_matches_exact_retained_bytes_without_mutation(tmp_path):
    path = tmp_path / "vehicles.csv"
    path.write_bytes(REPLAYED)
    expected = hashlib.sha256(RETAINED).hexdigest()
    assert _atb_replay_digest(path, expected, TRAJECTORIES) == expected
    assert path.read_bytes() == REPLAYED
    assert _atb_replay_digest(path, None, TRAJECTORIES) == hashlib.sha256(REPLAYED).hexdigest()


@pytest.mark.parametrize("content", [
    REPLAYED.replace(b"42.5", b"42.6"),
    REPLAYED.replace(b"Conservative,39.0,False\r\n", b""),
    REPLAYED.replace(b"value", b"different_unit"),
    REPLAYED.replace(b"Conservative,39.0,False", b"Conservative,39.0,True"),
])
def test_atb_value_coverage_schema_and_invalid_marker_changes_remain_fatal(tmp_path, content):
    path = tmp_path / "vehicles.csv"
    path.write_bytes(content)
    with pytest.raises(ValueError, match="Stale GREET ATB|Invalid ATB"):
        _atb_replay_digest(path, hashlib.sha256(RETAINED).hexdigest(), TRAJECTORIES)


@pytest.mark.parametrize("changed_value", [False, True])
def test_family_rename_preserves_exact_retained_digest_and_rejects_value_changes(
    tmp_path, changed_value,
):
    retained = (
        b"trajectory,value,is_default_trajectory,source_id\r\n"
        b"Mid,42.5,False,nlr_atb_transportation_2024\r\n"
        b"Conservative,39.0,True,nlr_atb_transportation_2024\r\n"
    )
    current = retained.replace(
        b"nlr_atb_transportation_2024", b"nlr_atb_transportation"
    ).replace(b"42.5,False", b"42.5,True").replace(b"39.0,True", b"39.0,False")
    if changed_value:
        current = current.replace(b"42.5", b"42.6")
    path = tmp_path / "vehicles.csv"
    path.write_bytes(current)
    expected = hashlib.sha256(retained).hexdigest()
    if changed_value:
        with pytest.raises(ValueError, match="Stale GREET ATB"):
            _atb_replay_digest(path, expected, TRAJECTORIES)
    else:
        assert _atb_replay_digest(path, expected, TRAJECTORIES) == expected
    assert path.read_bytes() == current


def test_only_the_frozen_greet_identity_is_normalized():
    assert _canonical_source_key("argonne_rd_greet_2025_rev1") == SOURCE
    assert _canonical_source_key(SOURCE) == SOURCE
    assert _canonical_source_key("unregistered_greet_release") != SOURCE
