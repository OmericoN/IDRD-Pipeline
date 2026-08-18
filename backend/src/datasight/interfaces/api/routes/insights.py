"""Insight export API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Response, status

from datasight.application.insights import (
    INSIGHT_COLUMNS,
    serialize_insights_csv,
    validate_insight_columns,
)
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.interfaces.api.schemas import InsightsResponse

router = APIRouter(tags=["insights"])


@router.get("/insights", response_model=InsightsResponse)
def insights(limit: int = Query(default=100, ge=1, le=1000)) -> InsightsResponse:
    with PipelineRepository() as repo:
        rows = repo.export_insight_rows()
    return InsightsResponse(columns=list(INSIGHT_COLUMNS), rows=rows[:limit])


@router.get("/insights/export.csv")
def export_insights_csv(columns: list[str] | None = Query(default=None)) -> Response:
    try:
        selected_columns = validate_insight_columns(columns)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc

    with PipelineRepository() as repo:
        rows = repo.export_insight_rows()
    csv_content = serialize_insights_csv(rows, selected_columns)
    return Response(
        content=csv_content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="datasight-insights.csv"'},
    )
