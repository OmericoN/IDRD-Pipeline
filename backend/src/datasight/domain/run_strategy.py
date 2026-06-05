"""Pipeline run execution strategies."""

from __future__ import annotations

from enum import StrEnum


class RunStrategy(StrEnum):
    STANDARD = "standard"
    HIGH_THROUGHPUT = "high_throughput"
