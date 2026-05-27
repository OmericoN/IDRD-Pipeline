"""Insight export API routes."""

from __future__ import annotations

from fastapi import APIRouter, Query

from idrd.infrastructure.persistence.repository import PipelineRepository
from idrd.interfaces.api.schemas import InsightsResponse

router = APIRouter(tags=["insights"])


@router.get("/insights", response_model=InsightsResponse)
def insights(limit: int = Query(default=100, ge=1, le=1000)) -> InsightsResponse:
    with PipelineRepository() as repo:
        rows = repo.export_insight_rows()
    return InsightsResponse(rows=rows[:limit])
