"""Insight export API routes."""

from __future__ import annotations

from fastapi import APIRouter, Query

from idrd.api.schemas import InsightsResponse
from idrd.infrastructure.persistence.repository import PipelineRepository

router = APIRouter(tags=["insights"])


@router.get("/insights", response_model=InsightsResponse)
def insights(limit: int = Query(default=100, ge=1, le=1000)) -> InsightsResponse:
    with PipelineRepository() as repo:
        rows = repo.export_insight_rows()
    return InsightsResponse(rows=rows[:limit])
