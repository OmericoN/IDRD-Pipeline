"""Full-pipeline orchestration shared by CLI and Celery."""

from __future__ import annotations

from typing import Any

from celery import chain

from idrd.pipeline import services
from idrd.pipeline.celery_app import celery_app
from idrd.storage.repository import PipelineRepository


def create_run(query: str, config: dict[str, Any]) -> int:
    with PipelineRepository() as repo:
        return repo.create_pipeline_run(query=query, config=config)


def run_all_local(
    query: str,
    limit: int,
    output: str,
    um_datasets_path: str | None = None,
    overwrite: bool = False,
    open_access_only: bool = True,
    fields_of_study: str | None = None,
) -> dict[str, Any]:
    config = {
        "limit": limit,
        "output": output,
        "overwrite": overwrite,
        "open_access_only": open_access_only,
        "fields_of_study": fields_of_study,
        "um_datasets_path": um_datasets_path,
    }
    pipeline_run_id = create_run(query, config)
    results: list[dict[str, Any]] = []

    try:
        if um_datasets_path:
            results.append({"stage": "import_um_datasets", **services.import_um_datasets(um_datasets_path)})
        results.extend(
            [
                services.discover_publications(
                    query=query,
                    limit=limit,
                    open_access_only=open_access_only,
                    fields_of_study=fields_of_study,
                    pipeline_run_id=pipeline_run_id,
                ),
                services.download_pdf_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id),
                services.grobid_convert_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id),
                services.render_document_batch(limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id),
                services.detect_mentions_batch(limit=limit, pipeline_run_id=pipeline_run_id),
                services.extract_features_from_candidates(limit=limit, pipeline_run_id=pipeline_run_id),
                services.match_um_dataset_batch(limit=limit, pipeline_run_id=pipeline_run_id),
                services.export_insights_csv(output, pipeline_run_id=pipeline_run_id),
            ]
        )
    except Exception:
        _finish_run(pipeline_run_id, "failed")
        raise

    _finish_run(pipeline_run_id, "successful")
    return {"pipeline_run_id": pipeline_run_id, "status": "successful", "results": results}


def enqueue_run_all(
    query: str,
    limit: int,
    output: str,
    um_datasets_path: str | None = None,
    overwrite: bool = False,
    open_access_only: bool = True,
    fields_of_study: str | None = None,
) -> dict[str, Any]:
    config = {
        "limit": limit,
        "output": output,
        "overwrite": overwrite,
        "open_access_only": open_access_only,
        "fields_of_study": fields_of_study,
        "um_datasets_path": um_datasets_path,
    }
    pipeline_run_id = create_run(query, config)
    if um_datasets_path:
        services.import_um_datasets(um_datasets_path)

    from idrd.pipeline import tasks

    workflow = chain(
        tasks.discover_publications.si(query, limit, open_access_only, fields_of_study, pipeline_run_id),
        tasks.download_pdf.si(limit, overwrite, pipeline_run_id),
        tasks.grobid_convert.si(limit, overwrite, False, pipeline_run_id),
        tasks.render_document.si(limit, overwrite, pipeline_run_id),
        tasks.detect_mentions.si(limit, pipeline_run_id),
        tasks.extract_features.si(limit, pipeline_run_id),
        tasks.match_um_dataset.si(limit, pipeline_run_id),
        tasks.export_insights.si(output, None, pipeline_run_id),
        tasks.finish_pipeline_run.si(pipeline_run_id, "successful"),
    )
    async_result = workflow.apply_async()
    task_id = getattr(async_result, "id", None)
    with PipelineRepository() as repo:
        repo.update_pipeline_run_task_id(pipeline_run_id, task_id)
    return {"pipeline_run_id": pipeline_run_id, "task_id": task_id, "status": "queued"}


def celery_worker_available(timeout: float = 1.0) -> bool:
    try:
        replies = celery_app.control.ping(timeout=timeout)
        return bool(replies)
    except Exception:
        return False


def _finish_run(pipeline_run_id: int, status: str) -> None:
    with PipelineRepository() as repo:
        repo.finish_pipeline_run(pipeline_run_id, status)
