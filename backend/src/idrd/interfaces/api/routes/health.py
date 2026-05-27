"""Health API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from idrd.interfaces.api.schemas import HealthResponse
from idrd.infrastructure.health.probes import celery_worker_available, grobid_ready, redis_ready
from idrd.infrastructure.persistence.repository import PipelineRepository

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    details: dict[str, Any] = {}
    database_ready = False
    try:
        with PipelineRepository() as repo:
            details["database"] = repo.healthcheck()
            database_ready = bool(details["database"].get("ready"))
    except Exception as exc:
        details["database_error"] = str(exc)

    redis_is_ready = redis_ready()
    worker_is_ready = celery_worker_available()
    grobid_is_ready = grobid_ready()
    details["redis_ready"] = redis_is_ready
    details["worker_ready"] = worker_is_ready
    details["grobid_ready"] = grobid_is_ready
    return HealthResponse(
        database_ready=database_ready,
        redis_ready=redis_is_ready,
        worker_ready=worker_is_ready,
        grobid_ready=grobid_is_ready,
        details=details,
    )
