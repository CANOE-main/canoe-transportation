"""Shared mechanics for trusted SQLite identifiers."""

import sqlite3
from pathlib import Path


def quote_identifier(identifier: str) -> str:
    """Quote one SQLite identifier by escaping embedded double quotes."""
    return '"' + identifier.replace('"', '""') + '"'


def open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    """Open an existing database without creating or modifying a reference file."""
    if not path.is_file():
        raise FileNotFoundError(f"Comparison database is missing: {path}")
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
