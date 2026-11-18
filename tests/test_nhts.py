"""Physical source validation precedes survey filtering and cannot fetch offline."""

import hashlib
import zipfile

import pytest

from fetching.nhts_ldv_travel import NhtsRequest, fetch_to_cache, read_tables, validate_cache


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "survey.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "trip.csv", "HOUSEID,PERSONID,TRIPID,TRPMILES\n1,1,1,-9\n1,1,2,10\n"
        )
    return NhtsRequest(
        component="public_use",
        url="https://nhts.ornl.gov/media/2022/download/csv.zip",
        cache_path=path,
        expected_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        expected_bytes=path.stat().st_size,
        kind="zip",
        archive_members=["trip.csv"],
        tables={
            "trip": {
                "member": "trip.csv",
                "expected_rows": 2,
                "keys": ["HOUSEID", "PERSONID", "TRIPID"],
            }
        },
    )


def test_source_read_preserves_native_negative_codes_and_row_provenance(source):
    frame = read_tables(
        source, {"trip": ["HOUSEID", "PERSONID", "TRIPID", "TRPMILES"]}
    )["trip"]
    assert frame.TRPMILES.tolist() == [-9, 10]
    assert frame.source_row.tolist() == [2, 3]
    assert frame.source_member.unique().tolist() == ["trip.csv"]


def test_corrupt_cache_rejected_without_download_or_replacement(source, monkeypatch):
    original = source.cache_path.read_bytes()
    source.cache_path.write_bytes(original + b"changed")
    monkeypatch.setattr(
        "fetching.nhts_ldv_travel.requests.get",
        lambda *a, **k: pytest.fail("Cached evidence must not be replaced"),
    )
    with pytest.raises(ValueError, match="pinned bytes"):
        fetch_to_cache(source)
    assert source.cache_path.read_bytes() == original + b"changed"


def test_archive_members_and_keys_validated_before_normalization(source):
    with pytest.raises(ValueError, match="member catalogue"):
        validate_cache(source.model_copy(update={"archive_members": ("wrong.csv",)}))
    with pytest.raises(ValueError, match="row count or native keys"):
        read_tables(
            source.model_copy(
                update={
                    "tables": {
                        "trip": source.tables["trip"].model_copy(
                            update={"keys": ("HOUSEID",)}
                        )
                    }
                }
            ),
            {"trip": ["HOUSEID", "PERSONID", "TRIPID", "TRPMILES"]},
        )


def test_invalid_source_contract_fails_before_io(source):
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="official HTTPS host"):
        NhtsRequest.model_validate(
            {**source.model_dump(), "url": "https://unregistered.example/survey.zip"}
        )


def test_missing_cache_has_explicit_offline_failure(source, monkeypatch):
    source.cache_path.unlink()
    monkeypatch.setattr(
        "fetching.nhts_ldv_travel.requests.get",
        lambda *a, **k: pytest.fail("Offline validation accessed network"),
    )
    with pytest.raises(FileNotFoundError, match="acquire with --download"):
        validate_cache(source)


def test_parser_rejects_missing_columns(source):
    with pytest.raises(ValueError, match="missing required physical columns"):
        read_tables(source, {"trip": ["HOUSEID", "nonexistent"]})
