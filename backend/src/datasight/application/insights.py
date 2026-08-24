"""Insight export contracts and CSV serialization."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any


INSIGHT_COLUMNS: tuple[str, ...] = (
    "paper_id",
    "publication_title",
    "publication_year",
    "discovery_mode",
    "discovery_methods",
    "dataset_name",
    "dataset_role",
    "reference_directness",
    "confidence",
    "evidence",
    "metadata",
    "match_status",
    "match_method",
    "match_score",
    "matched_fields",
    "candidate_um_dataset_ids",
    "review_required",
    "um_dataset_id",
    "um_dataset_title",
    "um_repository",
)

DISCOVERY_CANDIDATE_COLUMNS: tuple[str, ...] = (
    "paper_id",
    "title",
    "doi",
    "year",
    "source_url",
    "open_access_url",
    "oa_status",
    "cited_by_count",
    "primary_source_name",
    "candidate_strength",
    "evidence_tier",
    "evidence_reasons",
    "matched_um_dataset_ids",
    "pipeline_ready",
    "included",
    "exclusion_reason",
)


def validate_insight_columns(columns: Sequence[str] | None) -> list[str]:
    """Return selected columns in canonical order, rejecting unknown or empty selections."""
    if columns is None:
        return list(INSIGHT_COLUMNS)

    requested = {column for column in columns if column}
    unknown = sorted(requested.difference(INSIGHT_COLUMNS))
    if unknown:
        raise ValueError(f"Unknown insight columns: {', '.join(unknown)}")
    if not requested:
        raise ValueError("Select at least one insight column.")
    return [column for column in INSIGHT_COLUMNS if column in requested]


def validate_discovery_candidate_columns(columns: Sequence[str] | None) -> list[str]:
    """Return selected discovery columns in canonical order."""
    return _validate_columns(columns, DISCOVERY_CANDIDATE_COLUMNS, "discovery candidate")


def serialize_insights_csv(
    rows: Iterable[dict[str, Any]],
    columns: Sequence[str] | None = None,
) -> str:
    """Serialize insight rows as UTF-8 compatible CSV with JSON structured cells."""
    selected_columns = validate_insight_columns(columns)
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=selected_columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: _csv_cell(row.get(column)) for column in selected_columns})
    return output.getvalue()


def serialize_discovery_candidates_csv(
    rows: Iterable[dict[str, Any]],
    columns: Sequence[str] | None = None,
) -> str:
    """Serialize selected run discovery candidates as CSV."""
    selected_columns = validate_discovery_candidate_columns(columns)
    return _serialize_rows_csv(rows, selected_columns)


def _validate_columns(
    columns: Sequence[str] | None,
    allowed_columns: Sequence[str],
    label: str,
) -> list[str]:
    if columns is None:
        return list(allowed_columns)
    requested = {column for column in columns if column}
    unknown = sorted(requested.difference(allowed_columns))
    if unknown:
        raise ValueError(f"Unknown {label} columns: {', '.join(unknown)}")
    if not requested:
        raise ValueError("Select at least one column.")
    return [column for column in allowed_columns if column in requested]


def _serialize_rows_csv(rows: Iterable[dict[str, Any]], columns: Sequence[str]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: _csv_cell(row.get(column)) for column in columns})
    return output.getvalue()


def _csv_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_default)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)
