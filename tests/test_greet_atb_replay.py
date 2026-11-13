"""A run-selection annotation must not invalidate unchanged external-model inputs."""
import csv
import hashlib
import io

import pytest

from fetching.greet_vehicle_cycle import (
    SOURCE, _atb_replay_digest, _canonical_source_key, _result_bank_digest,
)


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


@pytest.mark.parametrize("retained_newline", [b"\n", b"\r\n"])
@pytest.mark.parametrize("replayed_newline", [b"\n", b"\r\n"])
def test_atb_replay_authenticates_both_platform_record_separators(
    tmp_path, retained_newline, replayed_newline,
):
    retained = RETAINED.replace(b"\r\n", retained_newline)
    replayed = REPLAYED.replace(b"\r\n", replayed_newline)
    path = tmp_path / "vehicles.csv"
    path.write_bytes(replayed)
    expected = hashlib.sha256(retained).hexdigest()
    assert _atb_replay_digest(path, expected, TRAJECTORIES) == expected
    assert path.read_bytes() == replayed


@pytest.mark.parametrize("retained_newline,replayed_newline", [("\r\n", "\n"), ("\n", "\r\n")])
@pytest.mark.parametrize("changed_field", [False, True])
def test_atb_record_translation_preserves_newlines_inside_quoted_field_values(
    tmp_path, retained_newline, replayed_newline, changed_field,
):
    def table(newline, *, replayed):
        text = io.StringIO(newline="")
        writer = csv.writer(text, lineterminator=newline)
        writer.writerow(["trajectory", "value", "is_default_trajectory", "note"])
        writer.writerow(["Mid", "42.5", str(replayed), "first line\r\nsecond line"])
        writer.writerow(["Conservative", "39.0", str(not replayed), "unchanged"])
        return text.getvalue().encode("utf-8")

    retained = table(retained_newline, replayed=False)
    replayed = table(replayed_newline, replayed=True)
    if changed_field:
        replayed = replayed.replace(b"first line\r\nsecond line", b"first line\nsecond line")
    path = tmp_path / "vehicles.csv"
    path.write_bytes(replayed)
    expected = hashlib.sha256(retained).hexdigest()
    if changed_field:
        with pytest.raises(ValueError, match="Stale GREET ATB"):
            _atb_replay_digest(path, expected, TRAJECTORIES)
    else:
        assert _atb_replay_digest(path, expected, TRAJECTORIES) == expected
    assert path.read_bytes() == replayed


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


@pytest.mark.parametrize("registered_newline", [b"\n", b"\r\n"])
@pytest.mark.parametrize("checkout_newline", [b"\n", b"\r\n"])
def test_result_bank_matches_original_digest_after_uniform_checkout_translation(
    tmp_path, registered_newline, checkout_newline,
):
    registered = b"case_id,value\n0,42.5\n1,39.0\n".replace(b"\n", registered_newline)
    checkout = registered.replace(registered_newline, checkout_newline)
    path = tmp_path / "results.csv"
    path.write_bytes(checkout)
    expected = hashlib.sha256(registered).hexdigest()
    assert _result_bank_digest(path, expected) == expected
    assert path.read_bytes() == checkout


@pytest.mark.parametrize("checkout", [
    b"case_id,value\n0,42.6\n1,39.0\n",
    b"case_id,value\n0,42.5 \n1,39.0\n",
    b"case_id,value\n0,42.5\n1,39.0\n\n",
    b"case_id,value\r\n0,42.5\n1,39.0\r\n",
    b"case_id,value\r0,42.5\r1,39.0\r",
])
def test_result_bank_rejects_value_whitespace_coverage_and_mixed_newline_changes(tmp_path, checkout):
    path = tmp_path / "results.csv"
    path.write_bytes(checkout)
    retained = b"case_id,value\r\n0,42.5\r\n1,39.0\r\n"
    with pytest.raises(ValueError, match="GREET result-bank hash"):
        _result_bank_digest(path, hashlib.sha256(retained).hexdigest())
