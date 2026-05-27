"""Celery task entrypoints for high-throughput stage execution."""

from __future__ import annotations

from typing import Any

from pipeline import services
from pipeline.celery_app import celery_app
from pipeline.repository import PipelineRepository


@celery_app.task(name="idrd.discover_publications", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def discover_publications(
    self,
    query: str,
    limit: int = 100,
    open_access_only: bool = True,
    fields_of_study: str | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.discover_publications(query, limit, open_access_only, fields_of_study, pipeline_run_id)


@celery_app.task(name="idrd.download_pdf", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def download_pdf(
    self,
    limit: int | None = None,
    overwrite: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.download_pdf_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="idrd.grobid_convert", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=2)
def grobid_convert(
    self,
    limit: int | None = None,
    overwrite: bool = False,
    delete_pdf: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.grobid_convert_batch(
        limit=limit,
        overwrite=overwrite,
        delete_pdf=delete_pdf,
        pipeline_run_id=pipeline_run_id,
    )


@celery_app.task(name="idrd.render_document", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def render_document(
    self,
    limit: int | None = None,
    overwrite: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.render_document_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="idrd.detect_mentions", bind=True)
def detect_mentions(
    self,
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.detect_mentions_batch(limit=limit, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="idrd.extract_features", bind=True)
def extract_features(
    self,
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.extract_features_from_candidates(limit=limit, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="idrd.match_um_dataset", bind=True)
def match_um_dataset(
    self,
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.match_um_dataset_batch(limit=limit, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="idrd.export_insights", bind=True)
def export_insights(
    self,
    output_path: str,
    rows: list[dict[str, Any]] | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.export_insights_csv(output_path, rows, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="idrd.finish_pipeline_run", bind=True)
def finish_pipeline_run(self, pipeline_run_id: int, status: str) -> dict[str, Any]:
    with PipelineRepository() as repo:
        repo.finish_pipeline_run(pipeline_run_id, status)
    return {"pipeline_run_id": pipeline_run_id, "status": status}
