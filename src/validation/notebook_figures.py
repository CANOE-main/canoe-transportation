"""Stable visible identities for diagnostic figures and their publication exports."""

from __future__ import annotations

import re

import altair as alt


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
