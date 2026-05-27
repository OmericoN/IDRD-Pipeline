"""Filesystem helpers for persisted artifact records."""

from __future__ import annotations

from pathlib import Path


def path_size(path: str | Path) -> int | None:
    try:
        return Path(path).stat().st_size
    except OSError:
        return None
