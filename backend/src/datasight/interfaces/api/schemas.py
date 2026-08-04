"""Pydantic schemas for the DataSight HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from datasight.application.reset import RESET_CONFIRMATION
from datasight.config import DEFAULT_UM_DATASETS_PATH
from datasight.domain.run_strategy import RunStrategy


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
    topic_ids: list[str] | None = None
    keyword_terms: list[str] | None = None
    mesh_terms: list[str] | None = None
    from_year: int | None = Field(default=None, ge=1800, le=3000)
    to_year: int | None = Field(default=None, ge=1800, le=3000)
    use_um_profile: bool = True
    open_access_only: bool = True
    overwrite: bool = False
    um_datasets_path: str | None = DEFAULT_UM_DATASETS_PATH
    output_path: str = "storage/exports/insights.csv"
    strategy: RunStrategy = RunStrategy.STANDARD


class StageRunCreateRequest(BaseModel):
    query: str | None = None
    limit: int | None = Field(default=None, ge=1, le=1000)
    topic_ids: list[str] | None = None
    keyword_terms: list[str] | None = None
    mesh_terms: list[str] | None = None
    from_year: int | None = Field(default=None, ge=1800, le=3000)
    to_year: int | None = Field(default=None, ge=1800, le=3000)
    use_um_profile: bool = True
    open_access_only: bool = True
    overwrite: bool = False
    delete_pdf: bool = False
    output_path: str | None = None


class RunCreateResponse(BaseModel):
    pipeline_run_id: int
    task_id: str | None = None
    status: Literal["queued", "running", "successful", "completed_with_errors", "failed", "skipped"]


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


class PipelineRunEventSummary(BaseModel):
    id: int
    pipeline_run_id: int
    stage: str | None = None
    level: str = "info"
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class RunEventsResponse(BaseModel):
    events: list[PipelineRunEventSummary]


class InsightsResponse(BaseModel):
    rows: list[dict[str, Any]]


class ImportUMDatasetsRequest(BaseModel):
    path: str = Field(min_length=1)


class ImportUMDatasetsResponse(BaseModel):
    status: str
    count: int
    deleted: int = 0
    path: str
    warnings: list[str] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)


class UMDatasetSummary(BaseModel):
    um_dataset_id: str
    title: str
    aliases: list[str] = Field(default_factory=list)
    creators: list[str] = Field(default_factory=list)
    doi: str | None = None
    url: str | None = None
    year: int | None = None
    repository: str | None = None
    keywords: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class UMDatasetDetail(UMDatasetSummary):
    raw: dict[str, Any] = Field(default_factory=dict)


class UMDatasetListResponse(BaseModel):
    items: list[UMDatasetSummary]
    total: int
    offset: int
    limit: int
    repositories: list[str] = Field(default_factory=list)
    years: list[int] = Field(default_factory=list)


class UMDatasetVerificationIssue(BaseModel):
    um_dataset_id: str
    title: str
    status: Literal["missing", "unexpected", "changed"]
    changed_fields: list[str] = Field(default_factory=list)


class UMDatasetVerificationResponse(BaseModel):
    status: Literal["verified", "mismatch", "unavailable"]
    source_path: str
    checked_at: datetime
    source_count: int | None = None
    stored_count: int
    verified_count: int
    issues: list[UMDatasetVerificationIssue] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    message: str | None = None


class ResetRequest(BaseModel):
    confirm: str = Field(description=f'Must equal "{RESET_CONFIRMATION}".')
    force: bool = False


class ResetResponse(BaseModel):
    status: str
    active_runs: int
    truncated_tables: list[str]
    deleted_paths: list[str]
    recreated_directories: list[str]
