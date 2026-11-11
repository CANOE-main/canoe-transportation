"""Stable visible identities for diagnostic figures and their publication exports."""

from __future__ import annotations

import re

import altair as alt


def source_markdown(text: str) -> str:
    """Qualify sources in displayed metadata prose while preserving quoted evidence."""
    citations = [
        ("NRCan CEUD", ("NRCan CEUD", "CEUD"), "https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"),
        ("NRCan Ratings", ("NRCan Ratings", "NRCan ratings", "NRCan"), "https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64"),
        ("Ontario MTO", ("Ontario MTO", "MTO"), "https://data.ontario.ca/dataset/vehicle-population-data"),
        ("NLR ATB", ("NLR ATB", "ATB"), "https://atb.nlr.gov/transportation/2024/data"),
        ("NHTSA CAFE", ("NHTSA CAFE", "NHTSA"), "https://www.nhtsa.gov/corporate-average-fuel-economy"),
        ("EIA NEMS", ("EIA NEMS", "NEMS"), "https://github.com/EIAgov/NEMS"),
        ("EPA MOVES", ("EPA MOVES", "MOVES"), "https://www.epa.gov/moves"),
        ("EPA OMEGA", ("EPA OMEGA", "OMEGA"), "https://github.com/USEPA/EPA_OMEGA_Model"),
        ("Transport Canada iZEV", ("Transport Canada iZEV", "iZEV"), "https://open.canada.ca/data/en/dataset/42986a95-be23-436e-af15-7c6bf292a2e1"),
        ("StatCan", ("StatCan",), "https://www.statcan.gc.ca/en/developers/wds"),
        ("ANL BEAN", ("ANL BEAN", "BEAN"), "https://publications.anl.gov/anlpubs/2023/11/179337.pdf"),
        ("ANL GREET", ("ANL GREET", "GREET"), "https://greet.anl.gov/greet/versions.html"),
        ("CER CEF", ("CER CEF", "CER"), "https://www.cer-rec.gc.ca/en/data-analysis/canada-energy-future/"),
    ]
    labels = {alias: (label, url) for label, aliases, url in citations for alias in aliases}
    aliases = "|".join(map(re.escape, sorted(labels, key=len, reverse=True)))
    held = []

    def keep(value):
        held.append(value)
        return f"\x00{len(held)-1}\x00"

    # These spans are source-native quotations or existing markup, not prose to restyle.
    protected = r"```[\s\S]*?```|`[^`\n]+`|\$\$[\s\S]*?\$\$|\\\[[\s\S]*?\\\]|\\\([\s\S]*?\\\)|(?<!\$)\$(?!\$)[^$\n]+\$(?!\$)|!?\[[^\]]*\]\([^\n]+?\)|https?://[^\s<>]+"
    result = re.sub(protected, lambda match: keep(match.group()), text)

    def link(source):
        label, url = labels[source]
        return f"[*{label}*]({url})"

    result = re.sub(r"(\*\*|\*)(" + aliases + r")\1", lambda match: keep(
        "**" + link(match[2]) + "**" if match[1] == "**" else link(match[2])), result)
    result = re.sub(r"(?<![A-Za-z])(?:" + aliases + r")(?![A-Za-z])", lambda match: link(match.group()), result)
    return re.sub(r"\x00(\d+)\x00", lambda match: held[int(match[1])], result)


def source_legend_style() -> alt.ExprRef:
    """Italicize source references without styling technology or method labels."""
    return alt.ExprRef(expr=(
        "test(/NRCan|CEUD|NLR ATB|ATB|Ontario MTO|MTO|NHTSA|NEMS|MOVES|OMEGA|StatCan/, "
        "datum.label) ? 'italic' : 'normal'"
    ))


def identified_chart(chart: alt.TopLevelMixin, figure_id: str, title: str) -> alt.TopLevelMixin:
    """Keep an explicit analytical identity independent of cell or display order."""
    identifier = re.sub(r"[^a-z0-9]+", "-", figure_id.lower()).strip("-")
    if not identifier or not title.strip():
        raise ValueError("Diagnostic figures require an identity and analytical title")
    previous = chart.title
    subtitles = getattr(previous, "subtitle", alt.Undefined)
    context = [] if subtitles is alt.Undefined else ([subtitles] if isinstance(subtitles, str) else list(subtitles))
    return chart.properties(
        title=alt.Title(title, subtitle=[f"Figure {identifier}", *context], anchor="start", fontSize=16,
                        subtitleFontSize=12, subtitleColor="#475569", offset=14),
        usermeta={"figure_id": identifier, "analytical_title": title},
    )
