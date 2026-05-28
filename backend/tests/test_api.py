from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.interfaces.api.main import app


client = TestClient(app)


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


def test_create_run_enqueues_pipeline(monkeypatch):
    def fake_enqueue_run_all(**kwargs):
        assert kwargs["query"] == "Maastricht dataset reuse"
        assert kwargs["limit"] == 5
        return {"pipeline_run_id": 7, "task_id": "task-1", "status": "queued"}

    monkeypatch.setattr("datasight.interfaces.api.routes.runs.orchestrator.enqueue_run_all", fake_enqueue_run_all)

    response = client.post(
        "/api/v1/runs",
        json={"query": "Maastricht dataset reuse", "limit": 5},
    )

    assert response.status_code == 202
    assert response.json() == {"pipeline_run_id": 7, "task_id": "task-1", "status": "queued"}


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
