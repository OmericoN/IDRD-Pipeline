"""Insight export API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Response, status

from datasight.application.insights import (
    INSIGHT_COLUMNS,
    serialize_insights_csv,
    validate_insight_columns,
)
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.interfaces.api.schemas import InsightsResponse, PaginatedInsightsResponse

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


@router.get("/runs/{pipeline_run_id}/insights", response_model=PaginatedInsightsResponse)
def run_insights(
    pipeline_run_id: int,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
) -> PaginatedInsightsResponse:
    with PipelineRepository() as repo:
        if not repo.get_pipeline_run(pipeline_run_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found.")
        rows = repo.export_insight_rows(pipeline_run_id=pipeline_run_id)
    return PaginatedInsightsResponse(
        columns=list(INSIGHT_COLUMNS),
        rows=rows[offset:offset + limit],
        total=len(rows),
        offset=offset,
        limit=limit,
    )


@router.get("/runs/{pipeline_run_id}/insights/export.csv")
def export_run_insights_csv(
    pipeline_run_id: int,
    columns: list[str] | None = Query(default=None),
) -> Response:
    try:
        selected_columns = validate_insight_columns(columns)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

    with PipelineRepository() as repo:
        if not repo.get_pipeline_run(pipeline_run_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found.")
        rows = repo.export_insight_rows(pipeline_run_id=pipeline_run_id)
    csv_content = serialize_insights_csv(rows, selected_columns)
    return Response(
        content=csv_content,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="datasight-run-{pipeline_run_id}-insights.csv"'
        },
    )
