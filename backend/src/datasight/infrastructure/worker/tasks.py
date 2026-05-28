"""Celery task entrypoints for high-throughput stage execution."""

from __future__ import annotations

from typing import Any

from celery.signals import task_failure

from datasight.application import pipeline_services as services
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.infrastructure.worker.celery_app import celery_app


@celery_app.task(name="datasight.discover_publications", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def discover_publications(
    self,
    query: str,
    limit: int = 100,
    open_access_only: bool = True,
    fields_of_study: str | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.discover_publications(query, limit, open_access_only, fields_of_study, pipeline_run_id)


@celery_app.task(name="datasight.download_pdf", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def download_pdf(
    self,
    limit: int | None = None,
    overwrite: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.download_pdf_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="datasight.grobid_convert", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=2)
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


@celery_app.task(name="datasight.render_document", bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def render_document(
    self,
    limit: int | None = None,
    overwrite: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.render_document_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="datasight.detect_mentions", bind=True)
def detect_mentions(
    self,
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.detect_mentions_batch(limit=limit, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="datasight.extract_features", bind=True)
def extract_features(
    self,
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.extract_features_from_candidates(limit=limit, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="datasight.match_um_dataset", bind=True)
def match_um_dataset(
    self,
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.match_um_dataset_batch(limit=limit, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="datasight.export_insights", bind=True)
def export_insights(
    self,
    output_path: str,
    rows: list[dict[str, Any]] | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    return services.export_insights_csv(output_path, rows, pipeline_run_id=pipeline_run_id)


@celery_app.task(name="datasight.finish_pipeline_run", bind=True)
def finish_pipeline_run(self, pipeline_run_id: int, status: str) -> dict[str, Any]:
    with PipelineRepository() as repo:
        repo.finish_pipeline_run(pipeline_run_id, status)
    return {"pipeline_run_id": pipeline_run_id, "status": status}


@task_failure.connect
def mark_pipeline_run_failed(
    sender=None,
    task_id: str | None = None,
    exception: BaseException | None = None,
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    **_: Any,
) -> None:
    if sender is None or not str(getattr(sender, "name", "")).startswith("datasight."):
        return
    pipeline_run_id = _extract_pipeline_run_id(args or (), kwargs or {})
    if pipeline_run_id is None:
        return
    error = str(exception) if exception else f"Task failed: {task_id}"
    with PipelineRepository() as repo:
        repo.fail_pipeline_run(pipeline_run_id, error)


def _extract_pipeline_run_id(args: tuple[Any, ...], kwargs: dict[str, Any]) -> int | None:
    value = kwargs.get("pipeline_run_id")
    if isinstance(value, int):
        return value
    if args and isinstance(args[-1], int):
        return args[-1]
    return None
