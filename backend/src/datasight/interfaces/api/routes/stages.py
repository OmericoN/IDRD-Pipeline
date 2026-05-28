"""Pipeline stage API routes."""

from __future__ import annotations

from fastapi import APIRouter

from datasight.application.stage_registry import stage_catalog
from datasight.interfaces.api.schemas import StageInfo, StagesResponse

router = APIRouter(tags=["stages"])


@router.get("/stages", response_model=StagesResponse)
def stages() -> StagesResponse:
    return StagesResponse(stages=[StageInfo.model_validate(stage) for stage in stage_catalog()])
