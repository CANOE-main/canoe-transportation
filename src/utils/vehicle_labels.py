"""Deterministic normalization and family comparison for vehicle labels."""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd


_ANNOTATION_PATTERN = re.compile(
    r"\s*\((?:tentative|generic[^)]*|model unresolved[^)]*)\)\s*",
    flags=re.IGNORECASE,
)
_UNRESOLVED_PATTERN = re.compile(
    r"^(?:model\s+)?(?:unresolved|unknown|not\s+resolved|n/?a)(?:\b|\s|$)",
    flags=re.IGNORECASE,
)


def normalize_vehicle_label(value: object) -> str:
    """Return a case-insensitive alphanumeric comparison form."""
    text = "" if pd.isna(value) else str(value)
    text = _ANNOTATION_PATTERN.sub(" ", text).casefold()
    return re.sub(r"[^a-z0-9]+", "", text)


def is_unresolved_vehicle_label(value: object) -> bool:
    """Return whether a candidate explicitly declines to identify a model."""
    text = "" if pd.isna(value) else str(value).strip()
    return not text or bool(_UNRESOLVED_PATTERN.match(text))


def candidate_options(value: object) -> list[str]:
    """Return a candidate plus slash-delimited alternatives in stable order."""
    text = "" if pd.isna(value) else str(value).strip()
    if not text:
        return []
    values = [text]
    if "/" in text:
        values.extend(part.strip() for part in text.split("/") if part.strip())
    output: list[str] = []
    seen: set[str] = set()
    for candidate in values:
        normalized = normalize_vehicle_label(candidate)
        if normalized and normalized not in seen:
            output.append(candidate)
            seen.add(normalized)
    return output


def vehicle_families_equivalent(left: object, right: object) -> bool:
    """Compare labels flexibly without treating short substrings as families."""
    left_normalized = normalize_vehicle_label(left)
    right_normalized = normalize_vehicle_label(right)
    if not left_normalized or not right_normalized:
        return False
    if left_normalized == right_normalized:
        return True
    shorter, longer = sorted(
        (left_normalized, right_normalized), key=lambda value: (len(value), value)
    )
    if len(shorter) >= 2 and longer.startswith(shorter):
        return True
    return len(shorter) >= 4 and shorter in longer


@dataclass(frozen=True)
class CandidateAgreement:
    """Auditable result of comparing two candidate passes."""

    status: str
    agreed_candidate: str


def reconcile_candidate_passes(first: object, second: object) -> CandidateAgreement:
    """Classify two passes and select the repeated slash alternative when present."""
    first_text = "" if pd.isna(first) else str(first).strip()
    second_text = "" if pd.isna(second) else str(second).strip()
    if first_text.casefold() == second_text.casefold():
        return CandidateAgreement("agreement", second_text)

    matches: list[tuple[int, int, str]] = []
    for first_option in candidate_options(first_text):
        for second_option in candidate_options(second_text):
            if not vehicle_families_equivalent(first_option, second_option):
                continue
            exact = int(
                normalize_vehicle_label(first_option)
                == normalize_vehicle_label(second_option)
            )
            scalar = second_option if "/" not in second_text else first_option
            matches.append((exact, len(normalize_vehicle_label(scalar)), scalar))
    if not matches:
        return CandidateAgreement("disagreement", "")
    _, _, selected = max(matches, key=lambda item: (item[0], item[1], item[2].casefold()))
    return CandidateAgreement("agreement", selected)


def candidate_matches_any(candidate: object, *passes: object) -> bool:
    """Return whether a selected family agrees with any candidate alternative."""
    return any(
        vehicle_families_equivalent(candidate, option)
        for value in passes
        for option in candidate_options(value)
    )
