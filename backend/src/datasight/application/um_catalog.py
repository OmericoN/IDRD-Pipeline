"""Read-only UM catalog integrity verification."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datasight.application.pipeline_services import load_um_dataset_records
from datasight.config import DEFAULT_UM_DATASETS_PATH, PROJECT_ROOT
from datasight.domain.schemas import UMDatasetRecord
from datasight.infrastructure.ingestion.openalex_exports import (
    load_um_openalex_export_bundle,
    looks_like_openalex_export,
)
from datasight.infrastructure.persistence.repository import PipelineRepository

COMPARISON_FIELDS = (
    "title",
    "aliases",
    "creators",
    "doi",
    "url",
    "year",
    "repository",
    "keywords",
    "raw",
)


def verify_um_dataset_catalog(source_path: str = DEFAULT_UM_DATASETS_PATH) -> dict[str, Any]:
    """Compare every configured source record with its durable database representation."""
    checked_at = datetime.now(UTC)
    with PipelineRepository() as repo:
        stored_records = repo.list_um_dataset_records()

    source = _project_path(source_path)
    try:
        if looks_like_openalex_export(source):
            bundle = load_um_openalex_export_bundle(source)
            source_records = bundle.records
            warnings = bundle.warnings
            metrics = bundle.metrics
        else:
            source_records = load_um_dataset_records(source_path)
            warnings = []
            metrics = {"source_rows": len(source_records)}
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        return {
            "status": "unavailable",
            "source_path": source_path,
            "checked_at": checked_at,
            "source_count": None,
            "stored_count": len(stored_records),
            "verified_count": 0,
            "issues": [],
            "warnings": [],
            "metrics": {},
            "message": str(exc),
        }

    source_by_id = {record.um_dataset_id: record for record in source_records}
    stored_by_id = {record.um_dataset_id: record for record in stored_records}
    issues: list[dict[str, Any]] = []
    verified_count = 0

    for um_dataset_id in sorted(source_by_id.keys() - stored_by_id.keys()):
        expected = source_by_id[um_dataset_id]
        issues.append(
            {
                "um_dataset_id": um_dataset_id,
                "title": expected.title,
                "status": "missing",
                "changed_fields": [],
            }
        )
    for um_dataset_id in sorted(stored_by_id.keys() - source_by_id.keys()):
        stored = stored_by_id[um_dataset_id]
        issues.append(
            {
                "um_dataset_id": um_dataset_id,
                "title": stored.title,
                "status": "unexpected",
                "changed_fields": [],
            }
        )
    for um_dataset_id in sorted(source_by_id.keys() & stored_by_id.keys()):
        expected = source_by_id[um_dataset_id].model_dump(mode="json")
        stored = stored_by_id[um_dataset_id].model_dump(mode="json")
        changed_fields = [field for field in COMPARISON_FIELDS if expected[field] != stored[field]]
        if changed_fields:
            issues.append(
                {
                    "um_dataset_id": um_dataset_id,
                    "title": source_by_id[um_dataset_id].title,
                    "status": "changed",
                    "changed_fields": changed_fields,
                }
            )
        else:
            verified_count += 1

    return {
        "status": "verified" if not issues else "mismatch",
        "source_path": source_path,
        "checked_at": checked_at,
        "source_count": len(source_records),
        "stored_count": len(stored_records),
        "verified_count": verified_count,
        "issues": issues,
        "warnings": warnings,
        "metrics": metrics,
        "message": None,
    }


def _project_path(path: str | Path) -> Path:
    resolved = Path(path)
    return resolved if resolved.is_absolute() else PROJECT_ROOT / resolved
