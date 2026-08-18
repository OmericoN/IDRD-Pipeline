from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.application import orchestrator


def test_run_outcome_surfaces_stage_errors():
    results = [
        {"stage": "discover", "status": "successful"},
        {"stage": "download_pdf", "status": "completed_with_errors"},
    ]

    assert orchestrator._run_outcome(results) == "completed_with_errors"


def test_enqueue_failure_marks_the_run_failed(monkeypatch):
    failed = []
    monkeypatch.setattr(orchestrator, "create_run", lambda query, config: 23)
    monkeypatch.setattr(
        orchestrator.services,
        "import_um_datasets",
        lambda path: None,
    )
    monkeypatch.setattr(
        orchestrator,
        "validate_discovery_preview",
        lambda preview_id, processing_limit: {
            "request": {"strategy_version": 2, "processing_limit": 5, "mode": "catalog_funnel"}
        },
    )

    class FailedWorkflow:
        def apply_async(self):
            raise ValueError("Queue unavailable")

    monkeypatch.setattr(orchestrator, "chain", lambda *tasks: FailedWorkflow())
    monkeypatch.setattr(
        orchestrator,
        "_fail_run",
        lambda run_id, error: failed.append((run_id, error)),
    )

    with pytest.raises(ValueError, match="Queue unavailable"):
        orchestrator.enqueue_run_all(
            preview_id="preview-123",
            processing_limit=5,
            output="storage/exports/insights.csv",
            um_datasets_path="data/um_dataset",
        )

    assert failed == [(23, "Queue unavailable")]
