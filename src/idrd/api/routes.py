"""Versioned HTTP routes for the IDRD backend."""

from __future__ import annotations

from typing import Any

import redis
import requests
from fastapi import APIRouter, HTTPException, Query, status

from idrd.api.schemas import (
    HealthResponse,
    ImportUMDatasetsRequest,
    ImportUMDatasetsResponse,
    InsightsResponse,
    PipelineRunSummary,
    ResetRequest,
    ResetResponse,
    RunCreateRequest,
    RunCreateResponse,
    RunEventsResponse,
    RunsResponse,
    StageInfo,
    StageRunCreateRequest,
    StagesResponse,
    PipelineRunEventSummary,
)
from idrd.config import CELERY_BROKER_URL, GROBID_ALIVE_CHECK_TIMEOUT_SEC, GROBID_BASE_URL
from idrd.pipeline import orchestrator, services, tasks
from idrd.pipeline.orchestrator import celery_worker_available
from idrd.pipeline.stages import PipelineStage, stage_values
from idrd.storage.repository import PipelineRepository
from idrd.storage.reset import RESET_CONFIRMATION, ResetBlockedError, reset_everything

router = APIRouter(prefix="/api/v1")

STAGE_DESCRIPTIONS: dict[str, str] = {
    "discover": "Find publication metadata and open-access PDF links.",
    "download_pdf": "Download PDF artifacts for discovered publications.",
    "grobid_convert": "Convert downloaded PDFs to TEI XML with GROBID.",
    "render_document": "Render TEI XML into markdown text for mention detection.",
    "detect_mentions": "Detect high-recall candidate dataset mentions.",
    "extract_features": "Promote candidates into structured mention records.",
    "match_um_dataset": "Match extracted mentions against imported UM dataset metadata.",
    "export_insights": "Export joined insight rows to CSV.",
}


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

    redis_ready = _redis_ready()
    worker_ready = celery_worker_available()
    grobid_ready = _grobid_ready()
    details["redis_ready"] = redis_ready
    details["worker_ready"] = worker_ready
    details["grobid_ready"] = grobid_ready
    return HealthResponse(
        database_ready=database_ready,
        redis_ready=redis_ready,
        worker_ready=worker_ready,
        grobid_ready=grobid_ready,
        details=details,
    )


@router.get("/stages", response_model=StagesResponse)
def stages() -> StagesResponse:
    return StagesResponse(
        stages=[
            StageInfo(name=stage, description=STAGE_DESCRIPTIONS.get(stage, stage))
            for stage in stage_values()
        ]
    )


@router.post("/runs", response_model=RunCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def create_run(request: RunCreateRequest) -> RunCreateResponse:
    result = orchestrator.enqueue_run_all(
        query=request.query,
        limit=request.limit,
        output=request.output_path,
        um_datasets_path=request.um_datasets_path,
        overwrite=request.overwrite,
        open_access_only=request.open_access_only,
        fields_of_study=request.fields_of_study,
    )
    return RunCreateResponse.model_validate(result)


@router.post("/stages/{stage}/runs", response_model=RunCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def create_stage_run(stage: PipelineStage, request: StageRunCreateRequest) -> RunCreateResponse:
    task = _task_for_stage(stage)
    stage_args = _stage_args(stage, request)
    stage_kwargs = _stage_kwargs(stage, request)
    with PipelineRepository() as repo:
        pipeline_run_id = repo.create_pipeline_run(
            query=request.query or stage.value,
            config={"stage": stage.value, **request.model_dump(mode="json")},
        )
    stage_kwargs["pipeline_run_id"] = pipeline_run_id
    async_result = task.delay(*stage_args, **stage_kwargs)
    task_id = getattr(async_result, "id", None)
    with PipelineRepository() as repo:
        repo.update_pipeline_run_task_id(pipeline_run_id, task_id)
    return RunCreateResponse(pipeline_run_id=pipeline_run_id, task_id=task_id, status="queued")


@router.get("/runs", response_model=RunsResponse)
def list_runs(limit: int = Query(default=25, ge=1, le=100)) -> RunsResponse:
    with PipelineRepository() as repo:
        runs = repo.list_pipeline_runs(limit=limit)
    return RunsResponse(runs=[PipelineRunSummary.model_validate(run) for run in runs])


@router.get("/runs/{run_id}", response_model=PipelineRunSummary)
def get_run(run_id: int) -> PipelineRunSummary:
    with PipelineRepository() as repo:
        run = repo.get_pipeline_run(run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found.")
    return PipelineRunSummary.model_validate(run)


@router.get("/runs/{run_id}/events", response_model=RunEventsResponse)
def get_run_events(run_id: int, limit: int = Query(default=200, ge=1, le=1000)) -> RunEventsResponse:
    with PipelineRepository() as repo:
        if not repo.get_pipeline_run(run_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found.")
        events = repo.list_pipeline_run_events(run_id, limit=limit)
    return RunEventsResponse(events=[PipelineRunEventSummary.model_validate(event) for event in events])


@router.get("/insights", response_model=InsightsResponse)
def insights(limit: int = Query(default=100, ge=1, le=1000)) -> InsightsResponse:
    with PipelineRepository() as repo:
        rows = repo.export_insight_rows()
    return InsightsResponse(rows=rows[:limit])


@router.post("/um-datasets/import", response_model=ImportUMDatasetsResponse)
def import_um_datasets(request: ImportUMDatasetsRequest) -> ImportUMDatasetsResponse:
    result = services.import_um_datasets(request.path)
    return ImportUMDatasetsResponse.model_validate(result)


@router.post("/admin/reset", response_model=ResetResponse)
def reset(request: ResetRequest) -> ResetResponse:
    if request.confirm != RESET_CONFIRMATION:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'confirm must equal "{RESET_CONFIRMATION}".',
        )
    try:
        result = reset_everything(force=request.force)
    except ResetBlockedError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return ResetResponse.model_validate(result)


def _redis_ready() -> bool:
    try:
        client = redis.Redis.from_url(CELERY_BROKER_URL, socket_connect_timeout=1, socket_timeout=1)
        return bool(client.ping())
    except Exception:
        return False


def _grobid_ready() -> bool:
    try:
        response = requests.get(
            f"{GROBID_BASE_URL}/api/isalive",
            timeout=GROBID_ALIVE_CHECK_TIMEOUT_SEC,
        )
        return response.ok
    except Exception:
        return False


def _task_for_stage(stage: PipelineStage) -> Any:
    return {
        PipelineStage.DISCOVER: tasks.discover_publications,
        PipelineStage.DOWNLOAD_PDF: tasks.download_pdf,
        PipelineStage.GROBID_CONVERT: tasks.grobid_convert,
        PipelineStage.RENDER_DOCUMENT: tasks.render_document,
        PipelineStage.DETECT_MENTIONS: tasks.detect_mentions,
        PipelineStage.EXTRACT_FEATURES: tasks.extract_features,
        PipelineStage.MATCH_UM_DATASET: tasks.match_um_dataset,
        PipelineStage.EXPORT_INSIGHTS: tasks.export_insights,
    }[stage]


def _stage_args(stage: PipelineStage, request: StageRunCreateRequest) -> list[Any]:
    if stage == PipelineStage.DISCOVER:
        if not request.query:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="query is required.")
        return [request.query]
    if stage == PipelineStage.EXPORT_INSIGHTS:
        if not request.output_path:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="output_path is required.")
        return [request.output_path]
    return []


def _stage_kwargs(stage: PipelineStage, request: StageRunCreateRequest) -> dict[str, Any]:
    if stage == PipelineStage.DISCOVER:
        return {
            "limit": request.limit or 100,
            "open_access_only": request.open_access_only,
            "fields_of_study": request.fields_of_study,
        }
    if stage in {PipelineStage.DOWNLOAD_PDF, PipelineStage.RENDER_DOCUMENT}:
        return {"limit": request.limit, "overwrite": request.overwrite}
    if stage == PipelineStage.GROBID_CONVERT:
        return {"limit": request.limit, "overwrite": request.overwrite, "delete_pdf": request.delete_pdf}
    if stage in {PipelineStage.DETECT_MENTIONS, PipelineStage.EXTRACT_FEATURES, PipelineStage.MATCH_UM_DATASET}:
        return {"limit": request.limit}
    return {}
