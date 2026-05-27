"""Administrative API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from idrd.api.schemas import ResetRequest, ResetResponse
from idrd.storage.reset import RESET_CONFIRMATION, ResetBlockedError, reset_everything

router = APIRouter(tags=["admin"])


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
