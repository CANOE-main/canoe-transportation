"""Failure and byte-contract checks for shared atomic text publication."""

import os
from pathlib import Path

import pytest

from utils import files


@pytest.mark.parametrize("text", ["", "Québec\nwarning\n"])
@pytest.mark.parametrize("newline", [None, "\n"])
def test_atomic_text_retains_utf8_and_caller_newlines(
    tmp_path: Path, text: str, newline: str | None,
) -> None:
    path = tmp_path / "artifacts" / "warnings.txt"

    files.write_text_atomic(text, path, newline=newline)

    expected = text if newline == "\n" else text.replace("\n", os.linesep)
    assert path.read_bytes() == expected.encode("utf-8")
    assert list(path.parent.iterdir()) == [path]


def test_encoding_failure_preserves_prior_artifact_and_removes_temporary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "warnings.txt"
    path.write_bytes(b"prior publication\n")

    with pytest.raises(UnicodeEncodeError):
        files.write_text_atomic("\ud800", path)

    assert path.read_bytes() == b"prior publication\n"
    assert list(tmp_path.iterdir()) == [path]


def test_replace_failure_preserves_prior_artifact_and_removes_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "metadata.json"
    path.write_bytes(b"prior publication\n")

    def reject_replace(temporary: Path, destination: Path) -> None:
        assert temporary.parent == path.parent
        assert temporary.read_bytes() == b"replacement\n"
        assert destination == path
        assert path.read_bytes() == b"prior publication\n"
        raise PermissionError("destination is open")

    monkeypatch.setattr(files.os, "replace", reject_replace)
    with pytest.raises(PermissionError, match="destination is open"):
        files.write_text_atomic("replacement\n", path, newline="\n")

    assert path.read_bytes() == b"prior publication\n"
    assert list(tmp_path.iterdir()) == [path]
