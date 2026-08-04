"""Read-only UM dataset catalog API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from datasight.application.um_catalog import verify_um_dataset_catalog
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.interfaces.api.schemas import (
    UMDatasetDetail,
    UMDatasetListResponse,
    UMDatasetVerificationResponse,
)

router = APIRouter(tags=["um-datasets"])


@router.get("/um-datasets", response_model=UMDatasetListResponse)
def list_um_datasets(
    q: str | None = Query(default=None, max_length=200),
    repository: str | None = Query(default=None, max_length=300),
    year: int | None = Query(default=None, ge=1800, le=3000),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
) -> UMDatasetListResponse:
    with PipelineRepository() as repo:
        result = repo.list_um_dataset_catalog(
            query=q.strip() if q and q.strip() else None,
            repository=repository,
            year=year,
            offset=offset,
            limit=limit,
        )
    return UMDatasetListResponse.model_validate(result)


@router.get("/um-datasets/verification", response_model=UMDatasetVerificationResponse)
def verify_um_datasets() -> UMDatasetVerificationResponse:
    return UMDatasetVerificationResponse.model_validate(verify_um_dataset_catalog())


@router.get("/um-datasets/{um_dataset_id}", response_model=UMDatasetDetail)
def get_um_dataset(um_dataset_id: str) -> UMDatasetDetail:
    with PipelineRepository() as repo:
        record = repo.get_um_dataset_catalog_record(um_dataset_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UM dataset not found.")
    return UMDatasetDetail.model_validate(record)
