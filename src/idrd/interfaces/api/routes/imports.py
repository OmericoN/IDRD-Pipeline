"""Reference data import API routes."""

from __future__ import annotations

from fastapi import APIRouter

from idrd.api.schemas import ImportUMDatasetsRequest, ImportUMDatasetsResponse
from idrd.application import pipeline_services

router = APIRouter(tags=["imports"])


@router.post("/um-datasets/import", response_model=ImportUMDatasetsResponse)
def import_um_datasets(request: ImportUMDatasetsRequest) -> ImportUMDatasetsResponse:
    result = pipeline_services.import_um_datasets(request.path)
    return ImportUMDatasetsResponse.model_validate(result)
