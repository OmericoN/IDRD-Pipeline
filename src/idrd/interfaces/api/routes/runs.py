"""Pipeline run API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from idrd.api.schemas import (
    PipelineRunEventSummary,
    PipelineRunSummary,
    RunCreateRequest,
    RunCreateResponse,
    RunEventsResponse,
    RunsResponse,
    StageRunCreateRequest,
)
from idrd.application import orchestrator
from idrd.application.stage_registry import (
    MissingStageArgument,
    PipelineStage,
    StageRunOptions,
    stage_args,
    stage_kwargs,
    task_for_stage,
)
from idrd.infrastructure.persistence.repository import PipelineRepository
from idrd.infrastructure.worker import tasks

router = APIRouter(tags=["runs"])


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
    task = task_for_stage(stage, tasks)
    with PipelineRepository() as repo:
        pipeline_run_id = repo.create_pipeline_run(
            query=request.query or stage.value,
            config={"stage": stage.value, **request.model_dump(mode="json")},
        )
    options = StageRunOptions(
        query=request.query,
        limit=request.limit,
        output_path=request.output_path,
        overwrite=request.overwrite,
        delete_pdf=request.delete_pdf,
        open_access_only=request.open_access_only,
        fields_of_study=request.fields_of_study,
        pipeline_run_id=pipeline_run_id,
    )
    try:
        async_result = task.delay(*stage_args(stage, options), **stage_kwargs(stage, options))
    except MissingStageArgument as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
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
