"""Shared mechanics for trusted SQLite identifiers."""


def quote_identifier(identifier: str) -> str:
    """Quote one SQLite identifier by escaping embedded double quotes."""
    return '"' + identifier.replace('"', '""') + '"'
