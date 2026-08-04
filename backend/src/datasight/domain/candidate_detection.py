"""Cheap first-pass dataset mention detection.

The detector intentionally favors recall. LLM extraction should run only on the
returned windows, not on entire papers.
"""

from __future__ import annotations

import re

from datasight.domain.schemas import MentionCandidate


DATASET_PATTERNS = [
    re.compile(r"\b(?P<name>[A-Z][A-Za-z0-9 ._-]{2,80})\s+(dataset|database|databank|registry|repository)\b"),
    re.compile(r"\b(dataset|database|databank|registry|repository)\s+(called|named|from|provided by)\s+(?P<name>[A-Z][A-Za-z0-9 ._-]{2,80})\b"),
    re.compile(r"\b(?P<name>[A-Z][A-Z0-9_-]{2,20})\b\s+(data|dataset|database)\b"),
]

SECTION_RE = re.compile(r"^(?P<hashes>#{1,6})\s+(?P<title>.+)$", re.MULTILINE)
SENTENCE_RE = re.compile(
    r"[^.!?\n]*(?:dataset|database|databank|registry|repository|data source|data were|data was|we collected|we gathered|survey responses (?:were|was|are) (?:collected|gathered|obtained))[^.!?\n]*[.!?]",
    re.I,
)


def detect_dataset_candidates(
    publication_id: str,
    markdown: str,
    min_score: float = 0.25,
) -> list[MentionCandidate]:
    section_spans = _section_spans(markdown)
    candidates: list[MentionCandidate] = []
    seen: set[tuple[str, int, int]] = set()

    for sentence_match in SENTENCE_RE.finditer(markdown):
        sentence = sentence_match.group(0).strip()
        if not sentence:
            continue

        names = _extract_names(sentence)
        if not names and _looks_like_implicit_dataset(sentence):
            names = ["Author-described dataset"]

        for name in names:
            start, end = sentence_match.span()
            key = (name.casefold(), start, end)
            if key in seen:
                continue
            seen.add(key)
            section = _section_for_offset(section_spans, start)
            score = _score_candidate(name, sentence, section)
            if score >= min_score:
                candidates.append(
                    MentionCandidate(
                        publication_id=publication_id,
                        dataset_name=name.strip(),
                        evidence_text=sentence,
                        section_heading=section,
                        standardized_section=standardize_section(section),
                        char_start=start,
                        char_end=end,
                        score=score,
                    )
                )

    return candidates


def standardize_section(section_heading: str | None) -> str | None:
    if not section_heading:
        return None
    text = section_heading.casefold()
    if "availability" in text:
        return "Data Availability"
    if any(token in text for token in ("method", "material", "data", "source", "cohort")):
        return "Methodology"
    if "abstract" in text:
        return "Abstract"
    if "result" in text:
        return "Results"
    if "discussion" in text:
        return "Discussion"
    if "introduction" in text:
        return "Introduction"
    return "Other"


def _extract_names(sentence: str) -> list[str]:
    names: list[str] = []
    for pattern in DATASET_PATTERNS:
        for match in pattern.finditer(sentence):
            name = match.groupdict().get("name")
            if name and len(name.strip()) > 2:
                names.append(_clean_name(name))
    return list(dict.fromkeys(names))


def _clean_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name).strip(" ,.;:()[]")
    name = re.sub(r"^(the|a|an)\s+", "", name, flags=re.I)
    return name


def _looks_like_implicit_dataset(sentence: str) -> bool:
    text = sentence.casefold()
    return any(
        phrase in text
        for phrase in (
            "we collected",
            "we gathered",
            "data were collected",
            "data was collected",
            "survey responses were collected",
            "survey responses were gathered",
            "survey responses were obtained",
            "interviews were",
        )
    )


def _score_candidate(name: str, sentence: str, section: str | None) -> float:
    score = 0.35
    if name != "Author-described dataset":
        score += 0.2
    if re.search(r"\b(doi|https?://|zenodo|dataverse|figshare|repository)\b", sentence, re.I):
        score += 0.2
    if standardize_section(section) == "Methodology":
        score += 0.15
    if re.search(r"\[[^\]]+\]|\([A-Z][A-Za-z]+ et al\.,? \d{4}\)", sentence):
        score += 0.1
    return min(score, 1.0)


def _section_spans(markdown: str) -> list[tuple[int, str]]:
    return [(match.start(), match.group("title").strip()) for match in SECTION_RE.finditer(markdown)]


def _section_for_offset(section_spans: list[tuple[int, str]], offset: int) -> str | None:
    current: str | None = None
    for start, title in section_spans:
        if start > offset:
            break
        current = title
    return current
