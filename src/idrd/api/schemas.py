"""Pydantic schemas for the IDRD HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from idrd.storage.reset import RESET_CONFIRMATION


class HealthResponse(BaseModel):
    database_ready: bool
    redis_ready: bool
    worker_ready: bool
    grobid_ready: bool
    details: dict[str, Any] = Field(default_factory=dict)


class StageInfo(BaseModel):
    name: str
    description: str


class StagesResponse(BaseModel):
    stages: list[StageInfo]


class RunCreateRequest(BaseModel):
    query: str = Field(min_length=1)
    limit: int = Field(default=100, ge=1, le=1000)
    fields_of_study: str | None = None
    open_access_only: bool = True
    overwrite: bool = False
    um_datasets_path: str | None = None
    output_path: str = "storage/exports/insights.csv"


class StageRunCreateRequest(BaseModel):
    query: str | None = None
    limit: int | None = Field(default=None, ge=1, le=1000)
    fields_of_study: str | None = None
    open_access_only: bool = True
    overwrite: bool = False
    delete_pdf: bool = False
    output_path: str | None = None


class RunCreateResponse(BaseModel):
    pipeline_run_id: int
    task_id: str | None = None
    status: Literal["queued", "running", "successful", "failed", "skipped"]


class StageRunSummary(BaseModel):
    id: int
    stage: str
    status: str
    attempt_count: int = 0
    task_id: str | None = None
    error: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PipelineRunSummary(BaseModel):
    id: int
    run_key: str
    query: str | None = None
    status: str
    config: dict[str, Any] = Field(default_factory=dict)
    celery_task_id: str | None = None
    error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    finished_at: datetime | None = None
    stages: list[StageRunSummary] = Field(default_factory=list)


class RunsResponse(BaseModel):
    runs: list[PipelineRunSummary]


class InsightsResponse(BaseModel):
    rows: list[dict[str, Any]]


class ImportUMDatasetsRequest(BaseModel):
    path: str = Field(min_length=1)


class ImportUMDatasetsResponse(BaseModel):
    status: str
    count: int
    path: str


class ResetRequest(BaseModel):
    confirm: str = Field(description=f'Must equal "{RESET_CONFIRMATION}".')
    force: bool = False


class ResetResponse(BaseModel):
    status: str
    active_runs: int
    truncated_tables: list[str]
    deleted_paths: list[str]
    recreated_directories: list[str]
