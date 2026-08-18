"""UM-optimized OpenAlex discovery planning and preview routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from datasight.application.discovery_preview import (
    DiscoveryPreviewError,
    create_discovery_preview,
    get_openalex_status,
    get_um_profile,
)
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.interfaces.api.schemas import (
    DiscoveryCandidateListResponse,
    DiscoveryPreviewRequest,
    DiscoveryPreviewResponse,
    OpenAlexStatusResponse,
    UMDiscoveryProfileResponse,
)

router = APIRouter(tags=["discovery"])


@router.get("/discovery/um-profile", response_model=UMDiscoveryProfileResponse)
def um_discovery_profile() -> UMDiscoveryProfileResponse:
    return UMDiscoveryProfileResponse.model_validate(get_um_profile())


@router.get("/openalex/status", response_model=OpenAlexStatusResponse)
def openalex_status() -> OpenAlexStatusResponse:
    return OpenAlexStatusResponse.model_validate(get_openalex_status())


@router.post("/discovery/preview", response_model=DiscoveryPreviewResponse)
def preview_discovery(request: DiscoveryPreviewRequest) -> DiscoveryPreviewResponse:
    try:
        preview = create_discovery_preview(request.model_dump(mode="json"))
    except DiscoveryPreviewError as exc:
        status_code = (
            status.HTTP_424_FAILED_DEPENDENCY
            if exc.kind in {"missing_api_key", "invalid_api_key"}
            else status.HTTP_409_CONFLICT
        )
        raise HTTPException(
            status_code=status_code,
            detail={"kind": exc.kind, "message": str(exc)},
        ) from exc
    return DiscoveryPreviewResponse.model_validate(preview)


@router.get(
    "/runs/{pipeline_run_id}/discovery-candidates",
    response_model=DiscoveryCandidateListResponse,
)
def run_discovery_candidates(
    pipeline_run_id: int,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
) -> DiscoveryCandidateListResponse:
    with PipelineRepository() as repo:
        if not repo.get_pipeline_run(pipeline_run_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found.")
        result = repo.list_discovery_candidates(pipeline_run_id, offset=offset, limit=limit)
    return DiscoveryCandidateListResponse.model_validate(result)
