import csv
import io
from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.interfaces.api.main import app
from datasight.application.insights import INSIGHT_COLUMNS


client = TestClient(app)


class FakeInsightRepo:
    rows = [
        {
            "paper_id": "W123",
            "publication_title": "A title, with punctuation",
            "discovery_mode": "catalog_funnel",
            "discovery_methods": ["dataset_citation", "title_mention"],
            "evidence": {"quote": "A reused dataset"},
        }
    ]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def export_insight_rows(self):
        return self.rows


def test_insights_returns_canonical_columns_with_limited_preview(monkeypatch):
    monkeypatch.setattr("datasight.interfaces.api.routes.insights.PipelineRepository", FakeInsightRepo)

    response = client.get("/api/v1/insights?limit=1")

    assert response.status_code == 200
    assert response.json()["columns"] == list(INSIGHT_COLUMNS)
    assert response.json()["rows"][0]["discovery_mode"] == "catalog_funnel"


def test_insight_csv_exports_all_rows_with_selected_canonical_columns(monkeypatch):
    monkeypatch.setattr("datasight.interfaces.api.routes.insights.PipelineRepository", FakeInsightRepo)

    response = client.get(
        "/api/v1/insights/export.csv?columns=discovery_methods&columns=paper_id"
    )

    assert response.status_code == 200
    assert response.headers["content-disposition"] == 'attachment; filename="datasight-insights.csv"'
    records = list(csv.DictReader(io.StringIO(response.text)))
    assert list(records[0]) == ["paper_id", "discovery_methods"]
    assert records[0]["paper_id"] == "W123"
    assert records[0]["discovery_methods"] == '["dataset_citation","title_mention"]'


def test_insight_csv_defaults_to_all_columns_and_rejects_invalid_selections(monkeypatch):
    monkeypatch.setattr("datasight.interfaces.api.routes.insights.PipelineRepository", FakeInsightRepo)

    response = client.get("/api/v1/insights/export.csv")
    invalid = client.get("/api/v1/insights/export.csv?columns=unknown")
    empty = client.get("/api/v1/insights/export.csv?columns=")

    assert next(csv.reader(io.StringIO(response.text))) == list(INSIGHT_COLUMNS)
    assert invalid.status_code == 422
    assert invalid.json()["detail"] == "Unknown insight columns: unknown"
    assert empty.status_code == 422
    assert empty.json()["detail"] == "Select at least one insight column."


def test_health_returns_polling_ready_shape(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def healthcheck(self):
            return {"ready": True, "alembic_revision": "20260527_0003"}

    monkeypatch.setattr("datasight.interfaces.api.routes.health.PipelineRepository", FakeRepo)
    monkeypatch.setattr("datasight.interfaces.api.routes.health.redis_ready", lambda: True)
    monkeypatch.setattr("datasight.interfaces.api.routes.health.celery_worker_available", lambda: True)
    monkeypatch.setattr("datasight.interfaces.api.routes.health.grobid_ready", lambda: False)

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json()["database_ready"] is True
    assert response.json()["redis_ready"] is True
    assert response.json()["worker_ready"] is True
    assert response.json()["grobid_ready"] is False


def test_create_run_enqueues_preview_backed_pipeline(monkeypatch):
    def fake_enqueue_run_all(**kwargs):
        assert kwargs["preview_id"] == "preview-123"
        assert kwargs["processing_limit"] == 5
        assert kwargs["excluded_candidate_ids"] == ["W901"]
        assert kwargs["um_datasets_path"] == "data/um_dataset"
        assert kwargs["strategy"] == "standard"
        return {"pipeline_run_id": 7, "task_id": "task-1", "status": "queued"}

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.orchestrator.enqueue_run_all", fake_enqueue_run_all)
    monkeypatch.setattr("datasight.interfaces.api.routes.runs.validate_discovery_preview", lambda *args: {})

    response = client.post(
        "/api/v1/runs",
        json={
            "preview_id": "preview-123",
            "processing_limit": 5,
            "excluded_candidate_ids": ["W901"],
        },
    )

    assert response.status_code == 202
    assert response.json() == {"pipeline_run_id": 7, "task_id": "task-1", "status": "queued"}


def test_create_run_accepts_high_throughput_strategy(monkeypatch):
    def fake_enqueue_run_all(**kwargs):
        assert kwargs["strategy"] == "high_throughput"
        assert kwargs["um_datasets_path"] == "data/um_dataset"
        return {"pipeline_run_id": 8, "task_id": "task-2", "status": "queued"}

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.orchestrator.enqueue_run_all", fake_enqueue_run_all)
    monkeypatch.setattr("datasight.interfaces.api.routes.runs.validate_discovery_preview", lambda *args: {})

    response = client.post(
        "/api/v1/runs",
        json={"preview_id": "preview-123", "strategy": "high_throughput"},
    )

    assert response.status_code == 202
    assert response.json()["pipeline_run_id"] == 8


def test_create_run_rejects_legacy_query_first_fields():
    response = client.post(
        "/api/v1/runs",
        json={
            "query": "UM Broad Discovery",
            "discovery_preset": "um_balanced",
            "preview_id": "preview-123",
        },
    )

    assert response.status_code == 422


def test_create_run_rejects_invalid_strategy():
    response = client.post(
        "/api/v1/runs",
        json={"preview_id": "preview-123", "strategy": "fast"},
    )

    assert response.status_code == 422


def test_discovery_profile_and_openalex_status_contracts(monkeypatch):
    profile = {
        "dataset_count": 2748,
        "catalog_fingerprint": "catalog-123",
        "coverage": {"openalex_ids": 100.0, "dois": 100.0},
        "counts": {"openalex_ids": 2748, "dois": 2748},
        "topic_resolution": {"status": "partial", "resolved": 9, "unresolved": 3},
        "phases": [],
        "top_topics": [],
        "top_keywords": [],
        "warnings": ["3 topics unresolved"],
    }
    monkeypatch.setattr("datasight.interfaces.api.routes.discovery.get_um_profile", lambda: profile)
    monkeypatch.setattr(
        "datasight.interfaces.api.routes.discovery.get_openalex_status",
        lambda: {
            "status": "invalid",
            "available": False,
            "remaining": None,
            "limit": None,
            "reset_seconds": None,
            "message": "The configured OpenAlex API key was rejected.",
        },
    )

    profile_response = client.get("/api/v1/discovery/um-profile")
    status_response = client.get("/api/v1/openalex/status")

    assert profile_response.status_code == 200
    assert profile_response.json()["dataset_count"] == 2748
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "invalid"
    assert "api_key" not in status_response.json()


def test_discovery_preview_rejects_removed_lane_contract():
    response = client.post(
        "/api/v1/discovery/preview",
        json={"strategy_version": 2, "mode": "catalog_funnel", "lanes": []},
    )

    assert response.status_code == 422


def test_discovery_candidate_page_returns_source_metadata(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def get_pipeline_run(self, pipeline_run_id):
            return {"id": pipeline_run_id}

        def list_discovery_candidates(self, pipeline_run_id, offset, limit):
            return {
                "items": [
                    {
                        "paper_id": "W900",
                        "title": "Candidate",
                        "doi": None,
                        "year": 2025,
                        "source_url": "https://openalex.org/W900",
                        "open_access_url": "https://example.org/paper.pdf",
                        "oa_status": "green",
                        "cited_by_count": 4,
                        "primary_source_name": "Research Data Journal",
                        "candidate_strength": 95.0,
                        "evidence_tier": "direct",
                        "evidence_reasons": ["dataset_citation"],
                        "matched_um_dataset_ids": ["W1"],
                        "pipeline_ready": True,
                        "included": True,
                        "exclusion_reason": None,
                    }
                ],
                "total": 1,
                "offset": offset,
                "limit": limit,
            }

    monkeypatch.setattr("datasight.interfaces.api.routes.discovery.PipelineRepository", FakeRepo)

    response = client.get("/api/v1/runs/9/discovery-candidates")

    assert response.status_code == 200
    assert response.json()["items"][0]["primary_source_name"] == "Research Data Journal"


def test_stage_run_validates_required_arguments_before_creating_run(monkeypatch):
    def unexpected_repository():
        raise AssertionError("A run must not be created for an invalid request.")

    monkeypatch.setattr(
        "datasight.interfaces.api.routes.runs.PipelineRepository",
        unexpected_repository,
    )

    response = client.post("/api/v1/stages/discover/runs", json={})

    assert response.status_code == 422
    assert response.json()["detail"] == "query is required."


def test_stage_run_marks_created_run_failed_when_queueing_fails(monkeypatch):
    class FakeRepo:
        failed = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def create_pipeline_run(self, query, config):
            return 17

        def fail_pipeline_run(self, run_id, error):
            self.failed.append((run_id, error))

    def queue_failure(*args, **kwargs):
        raise RuntimeError("Redis unavailable")

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.PipelineRepository", FakeRepo)
    monkeypatch.setattr(
        "datasight.interfaces.api.routes.runs.tasks.discover_publications.delay",
        queue_failure,
    )

    response = client.post(
        "/api/v1/stages/discover/runs",
        json={"query": "dataset reuse"},
    )

    assert response.status_code == 503
    assert FakeRepo.failed == [(17, "Redis unavailable")]


def test_get_run_returns_stage_progress(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def get_pipeline_run(self, run_id):
            assert run_id == 42
            return {
                "id": 42,
                "run_key": "run-42",
                "query": "dataset reuse",
                "status": "running",
                "config": {},
                "celery_task_id": "task-42",
                "stages": [
                    {
                        "id": 1,
                        "stage": "discover",
                        "status": "successful",
                        "attempt_count": 1,
                        "metrics": {"count": 3},
                    }
                ],
            }

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.PipelineRepository", FakeRepo)

    response = client.get("/api/v1/runs/42")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == 42
    assert body["stages"][0]["stage"] == "discover"


def test_get_run_events_returns_structured_log_entries(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def get_pipeline_run(self, run_id):
            assert run_id == 42
            return {"id": 42, "run_key": "run-42", "status": "running"}

        def list_pipeline_run_events(self, run_id, limit=200):
            assert run_id == 42
            assert limit == 50
            return [
                {
                    "id": 1,
                    "pipeline_run_id": 42,
                    "stage": "discover",
                    "level": "info",
                    "message": "Discovered 3 publications.",
                    "payload": {"count": 3},
                }
            ]

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.PipelineRepository", FakeRepo)

    response = client.get("/api/v1/runs/42/events?limit=50")

    assert response.status_code == 200
    assert response.json()["events"][0]["message"] == "Discovered 3 publications."


def test_get_run_events_rejects_unknown_run(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def get_pipeline_run(self, run_id):
            return None

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.PipelineRepository", FakeRepo)

    response = client.get("/api/v1/runs/404/events")

    assert response.status_code == 404


def test_list_um_datasets_returns_paginated_catalog(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def list_um_dataset_catalog(self, **kwargs):
            assert kwargs == {
                "query": "health",
                "repository": "Dataverse",
                "year": 2024,
                "offset": 50,
                "limit": 25,
            }
            return {
                "items": [{"um_dataset_id": "W1", "title": "Health Dataset"}],
                "total": 1,
                "offset": 50,
                "limit": 25,
                "repositories": ["Dataverse"],
                "years": [2024],
            }

    monkeypatch.setattr("datasight.interfaces.api.routes.um_datasets.PipelineRepository", FakeRepo)
    response = client.get(
        "/api/v1/um-datasets?q=health&repository=Dataverse&year=2024&offset=50&limit=25"
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["um_dataset_id"] == "W1"


def test_get_um_dataset_returns_404_for_unknown_id(monkeypatch):
    class FakeRepo:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def get_um_dataset_catalog_record(self, um_dataset_id):
            return None

    monkeypatch.setattr("datasight.interfaces.api.routes.um_datasets.PipelineRepository", FakeRepo)

    response = client.get("/api/v1/um-datasets/W404")

    assert response.status_code == 404


def test_verify_um_datasets_returns_integrity_result(monkeypatch):
    monkeypatch.setattr(
        "datasight.interfaces.api.routes.um_datasets.verify_um_dataset_catalog",
        lambda: {
            "status": "verified",
            "source_path": "data/um_dataset",
            "checked_at": "2026-07-21T00:00:00Z",
            "source_count": 2748,
            "stored_count": 2748,
            "verified_count": 2748,
            "issues": [],
            "warnings": [],
            "metrics": {"source_rows": 2748},
            "message": None,
        },
    )

    response = client.get("/api/v1/um-datasets/verification")

    assert response.status_code == 200
    assert response.json()["verified_count"] == 2748


def test_reset_rejects_wrong_confirmation():
    response = client.post("/api/v1/admin/reset", json={"confirm": "wrong", "force": True})

    assert response.status_code == 400


def test_reset_calls_service_with_confirmation(monkeypatch):
    monkeypatch.setattr(
        "datasight.interfaces.api.routes.admin.reset_everything",
        lambda force=False: {
            "status": "successful",
            "active_runs": 0,
            "truncated_tables": ["publications"],
            "deleted_paths": [],
            "recreated_directories": [],
        },
    )

    response = client.post("/api/v1/admin/reset", json={"confirm": "RESET DATASIGHT", "force": True})

    assert response.status_code == 200
    assert response.json()["status"] == "successful"
