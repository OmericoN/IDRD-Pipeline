"""Normalization helpers shared by candidate detection and UM matching."""

from __future__ import annotations

import re
import unicodedata


_DOI_PREFIX = re.compile(r"^(https?://(dx\.)?doi\.org/|doi:\s*)", re.I)
_SPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s./:-]+", re.UNICODE)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.casefold()
    text = _PUNCT_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def normalize_identifier(value: str | None) -> str:
    if not value:
        return ""
    identifier = value.strip()
    identifier = _DOI_PREFIX.sub("", identifier).strip()
    return identifier.rstrip(".").casefold()


def token_set(value: str | None) -> set[str]:
    return {token for token in normalize_text(value).split(" ") if token}


def jaccard(left: str | None, right: str | None) -> float:
    left_tokens = token_set(left)
    right_tokens = token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
