"""Resolve publication tables from explicit identity and structural evidence.

Label similarity is diagnostic only. A renamed table can be resolved only inside
the selected section, with an exact normalized header and regional signature.
"""

from dataclasses import dataclass, field
from difflib import get_close_matches
from html.parser import HTMLParser
import unicodedata


class PublicationTableError(ValueError):
    """A publication table is missing, ambiguous, or violates its identity contract."""


def normalize_publication_label(value: str) -> str:
    """Normalize presentation differences without dropping words or numbers."""
    text = unicodedata.normalize("NFKC", value).casefold()
    return " ".join("".join(char if char.isalnum() else " " for char in text).split())


@dataclass
class PublicationTable:
    """Parsed source-native cells and the evidence used to select their table."""

    headers: list[str]
    rows: list[list[str]]
    selection: dict


@dataclass
class _Heading:
    level: int
    identifier: str | None
    text: str = ""


@dataclass
class _Table:
    identifier: str | None
    section: tuple[int, ...]
    headers: list[list[str]] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    row: list[str] = field(default_factory=list)
    row_is_header: bool = False
    cell_kind: str | None = None
    cell_text: list[str] = field(default_factory=list)


class _PublicationParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.headings: list[_Heading] = []
        self.tables: list[_Table] = []
        self.section: list[int] = []
        self.heading: _Heading | None = None
        self.heading_text: list[str] = []
        self.active_tables: list[_Table] = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self.heading = _Heading(int(tag[1]), attributes.get("id"))
            self.heading_text = []
        elif tag == "table":
            table = _Table(attributes.get("id"), tuple(self.section))
            if self.active_tables:
                table.errors.append("nested tables are unsupported")
                self.active_tables[-1].errors.append("nested tables are unsupported")
            self.tables.append(table)
            self.active_tables.append(table)
        elif self.active_tables:
            table = self.active_tables[-1]
            if tag == "tr":
                table.row, table.row_is_header = [], False
            elif tag in {"th", "td"}:
                if any(attributes.get(key, "1") != "1" for key in ("colspan", "rowspan")):
                    table.errors.append("spanning cells are unsupported")
                table.cell_kind, table.cell_text = tag, []

    def handle_data(self, data):
        if self.heading is not None:
            self.heading_text.append(data)
        if self.active_tables and self.active_tables[-1].cell_kind is not None:
            self.active_tables[-1].cell_text.append(data)

    def handle_endtag(self, tag):
        if self.heading is not None and tag == f"h{self.heading.level}":
            self.heading.text = " ".join(" ".join(self.heading_text).split())
            while self.section and self.headings[self.section[-1]].level >= self.heading.level:
                self.section.pop()
            self.headings.append(self.heading)
            self.section.append(len(self.headings) - 1)
            self.heading = None
        if not self.active_tables:
            return
        table = self.active_tables[-1]
        if tag in {"th", "td"} and table.cell_kind == tag:
            table.row.append(" ".join(" ".join(table.cell_text).split()))
            table.row_is_header |= tag == "th"
            table.cell_kind = None
        elif tag == "tr" and table.row:
            (table.headers if table.row_is_header else table.rows).append(table.row)
            table.row = []
        elif tag == "table":
            self.active_tables.pop()

    def close(self):
        super().close()
        for table in self.active_tables:
            table.errors.append("table is not closed")


def _signature(table, required_headers, source_regions):
    if table.errors or len(table.headers) != 1 or not table.rows:
        return False
    headers = [normalize_publication_label(label) for label in table.headers[0]]
    if not all(headers) or len(set(headers)) != len(headers):
        return False
    if any(len(row) != len(headers) for row in table.rows):
        return False
    if not set(map(normalize_publication_label, required_headers)).issubset(headers):
        return False
    if source_regions is not None:
        regions = [normalize_publication_label(row[0]) for row in table.rows]
        if len(regions) != len(set(regions)) or set(regions) != set(map(normalize_publication_label, source_regions)):
            return False
    return True


def _suggestions(expected, values):
    labels = {normalize_publication_label(value): value for value in values if value}
    matches = get_close_matches(normalize_publication_label(expected), labels, n=3, cutoff=0.5)
    return [labels[match] for match in matches]


def select_publication_table(
    html: str,
    *,
    table_id: str,
    heading_id: str,
    heading_text: str | None = None,
    table_id_aliases=(),
    heading_id_aliases=(),
    required_headers=(),
    source_regions=None,
) -> PublicationTable:
    """Select a unique table using identity, section, headers and coverage.

    Explicit IDs take priority and must belong to the selected heading section.
    Missing heading IDs may use exact normalized configured text. Missing table
    IDs may use a unique in-section signature only when both required headers and
    expected regions are supplied. Fuzzy matches appear only in error messages.
    """
    parser = _PublicationParser()
    for name, values in (("table_id_aliases", table_id_aliases),
                         ("heading_id_aliases", heading_id_aliases),
                         ("required_headers", required_headers),
                         ("source_regions", source_regions)):
        if values is not None and (not isinstance(values, (list, tuple))
                                   or any(not isinstance(value, str) or not value.strip() for value in values)):
            label = "regions" if name == "source_regions" else name
            raise PublicationTableError(f"Configured publication {label} must be a sequence of nonempty labels")
    if source_regions is not None:
        normalized_regions = list(map(normalize_publication_label, source_regions))
        if not normalized_regions or not all(normalized_regions) or len(set(normalized_regions)) != len(normalized_regions):
            raise PublicationTableError("Configured publication regions must be nonempty and unique")
    parser.feed(html)
    parser.close()
    heading_ids = {heading_id, *heading_id_aliases}
    headings = [index for index, value in enumerate(parser.headings) if value.identifier in heading_ids]
    heading_method = "id"
    if not headings and heading_text is not None:
        headings = [index for index, value in enumerate(parser.headings)
                    if normalize_publication_label(value.text) == normalize_publication_label(heading_text)]
        heading_method = "normalized_text"
    if len(headings) != 1:
        suggestions = _suggestions(heading_text or heading_id, [value.text for value in parser.headings])
        raise PublicationTableError(
            f"Publication heading {heading_id!r} is missing or ambiguous; suggestions only: {suggestions}"
        )
    heading_index = headings[0]
    heading = parser.headings[heading_index]
    if heading_text is not None and normalize_publication_label(heading.text) != normalize_publication_label(heading_text):
        raise PublicationTableError(f"Publication heading {heading_id!r} text changed: {heading.text!r}")
    table_ids = {table_id, *table_id_aliases}
    tables = [value for value in parser.tables if value.identifier in table_ids]
    table_method = "id"
    if tables and any(heading_index not in table.section for table in tables):
        raise PublicationTableError(f"Publication table {table_id!r} is outside its configured heading section")
    if not tables and required_headers and source_regions is not None:
        tables = [value for value in parser.tables
                  if heading_index in value.section and _signature(value, required_headers, source_regions)]
        table_method = "section_signature"
    if len(tables) != 1:
        suggestions = _suggestions(table_id, [value.identifier for value in parser.tables])
        raise PublicationTableError(
            f"Publication table {table_id!r} is missing or ambiguous; suggestions only: {suggestions}"
        )
    table = tables[0]
    if not _signature(table, required_headers, source_regions):
        raise PublicationTableError(
            f"Publication table {table.identifier!r} headers changed or regional/row structure is incomplete: "
            f"{table.headers}; {table.errors}"
        )
    return PublicationTable(table.headers[0], table.rows, {
        "heading_id": heading.identifier,
        "heading_text": heading.text,
        "heading_resolution": heading_method,
        "table_id": table.identifier,
        "table_resolution": table_method,
        "headers": table.headers[0],
        "source_regions": [row[0] for row in table.rows],
        "section_heading_ids": [parser.headings[index].identifier for index in table.section],
    })
