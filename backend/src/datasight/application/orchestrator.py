"""Full-pipeline orchestration shared by CLI and Celery."""

from __future__ import annotations

from typing import Any

from celery import chain

from datasight.application import pipeline_services as services
from datasight.config import DEFAULT_UM_DATASETS_PATH
from datasight.domain.run_strategy import RunStrategy
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.infrastructure.worker.celery_app import celery_app


def create_run(query: str, config: dict[str, Any]) -> int:
    with PipelineRepository() as repo:
        return repo.create_pipeline_run(query=query, config=config)


def run_all_local(
    query: str,
    limit: int,
    output: str,
    um_datasets_path: str | None = DEFAULT_UM_DATASETS_PATH,
    overwrite: bool = False,
    open_access_only: bool = True,
    topic_ids: str | list[str] | None = None,
    keyword_terms: str | list[str] | None = None,
    mesh_terms: str | list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    use_um_profile: bool = True,
) -> dict[str, Any]:
    config = {
        "limit": limit,
        "output": output,
        "overwrite": overwrite,
        "open_access_only": open_access_only,
        "topic_ids": topic_ids,
        "keyword_terms": keyword_terms,
        "mesh_terms": mesh_terms,
        "from_year": from_year,
        "to_year": to_year,
        "use_um_profile": use_um_profile,
        "um_datasets_path": um_datasets_path,
    }
    pipeline_run_id = create_run(query, config)
    results: list[dict[str, Any]] = []

    try:
        if um_datasets_path:
            results.append(
                {
                    "stage": "import_um_datasets",
                    **services.import_um_datasets(um_datasets_path),
                }
            )
        results.extend(
            [
                services.discover_publications(
                    query=query,
                    limit=limit,
                    open_access_only=open_access_only,
                    topic_ids=topic_ids,
                    keyword_terms=keyword_terms,
                    mesh_terms=mesh_terms,
                    from_year=from_year,
                    to_year=to_year,
                    use_um_profile=use_um_profile,
                    pipeline_run_id=pipeline_run_id,
                ),
                services.download_pdf_batch(
                    limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id
                ),
                services.grobid_convert_batch(
                    limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id
                ),
                services.render_document_batch(
                    limit=limit, overwrite=overwrite, pipeline_run_id=pipeline_run_id
                ),
                services.detect_mentions_batch(
                    limit=limit, pipeline_run_id=pipeline_run_id
                ),
                services.extract_features_from_candidates(
                    limit=limit, pipeline_run_id=pipeline_run_id
                ),
                services.match_um_dataset_batch(
                    limit=limit, pipeline_run_id=pipeline_run_id
                ),
                services.export_insights_csv(output, pipeline_run_id=pipeline_run_id),
            ]
        )
    except Exception:
        _finish_run(pipeline_run_id, "failed")
        raise

    status = _run_outcome(results)
    _finish_run(pipeline_run_id, status)
    return {
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "results": results,
    }


def enqueue_run_all(
    query: str,
    limit: int,
    output: str,
    um_datasets_path: str | None = DEFAULT_UM_DATASETS_PATH,
    overwrite: bool = False,
    open_access_only: bool = True,
    topic_ids: str | list[str] | None = None,
    keyword_terms: str | list[str] | None = None,
    mesh_terms: str | list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    use_um_profile: bool = True,
    strategy: RunStrategy | str = RunStrategy.STANDARD,
) -> dict[str, Any]:
    strategy_value = RunStrategy(strategy)
    if strategy_value == RunStrategy.HIGH_THROUGHPUT:
        return enqueue_high_throughput_run(
            query=query,
            limit=limit,
            output=output,
            um_datasets_path=um_datasets_path,
            overwrite=overwrite,
            open_access_only=open_access_only,
            topic_ids=topic_ids,
            keyword_terms=keyword_terms,
            mesh_terms=mesh_terms,
            from_year=from_year,
            to_year=to_year,
            use_um_profile=use_um_profile,
        )

    config = {
        "limit": limit,
        "output": output,
        "overwrite": overwrite,
        "open_access_only": open_access_only,
        "topic_ids": topic_ids,
        "keyword_terms": keyword_terms,
        "mesh_terms": mesh_terms,
        "from_year": from_year,
        "to_year": to_year,
        "use_um_profile": use_um_profile,
        "um_datasets_path": um_datasets_path,
        "strategy": strategy_value.value,
    }
    pipeline_run_id = create_run(query, config)
    try:
        if um_datasets_path:
            services.import_um_datasets(um_datasets_path)

        from datasight.infrastructure.worker import tasks

        workflow = chain(
            tasks.discover_publications.si(
                query,
                limit,
                open_access_only,
                topic_ids,
                keyword_terms,
                mesh_terms,
                from_year,
                to_year,
                use_um_profile,
                pipeline_run_id,
            ),
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
    except Exception as exc:
        _fail_run(pipeline_run_id, str(exc))
        raise
    task_id = getattr(async_result, "id", None)
    with PipelineRepository() as repo:
        repo.update_pipeline_run_task_id(pipeline_run_id, task_id)
    return {"pipeline_run_id": pipeline_run_id, "task_id": task_id, "status": "queued"}


def enqueue_high_throughput_run(
    query: str,
    limit: int,
    output: str,
    um_datasets_path: str | None = DEFAULT_UM_DATASETS_PATH,
    overwrite: bool = False,
    open_access_only: bool = True,
    topic_ids: str | list[str] | None = None,
    keyword_terms: str | list[str] | None = None,
    mesh_terms: str | list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    use_um_profile: bool = True,
) -> dict[str, Any]:
    config = {
        "limit": limit,
        "output": output,
        "overwrite": overwrite,
        "open_access_only": open_access_only,
        "topic_ids": topic_ids,
        "keyword_terms": keyword_terms,
        "mesh_terms": mesh_terms,
        "from_year": from_year,
        "to_year": to_year,
        "use_um_profile": use_um_profile,
        "um_datasets_path": um_datasets_path,
        "strategy": RunStrategy.HIGH_THROUGHPUT.value,
    }
    pipeline_run_id = create_run(query, config)

    from datasight.infrastructure.worker import tasks

    try:
        async_result = tasks.bootstrap_high_throughput_run.apply_async(
            kwargs={
                "query": query,
                "limit": limit,
                "output_path": output,
                "um_datasets_path": um_datasets_path,
                "overwrite": overwrite,
                "open_access_only": open_access_only,
                "topic_ids": topic_ids,
                "keyword_terms": keyword_terms,
                "mesh_terms": mesh_terms,
                "from_year": from_year,
                "to_year": to_year,
                "use_um_profile": use_um_profile,
                "pipeline_run_id": pipeline_run_id,
            },
            queue="processing",
        )
    except Exception as exc:
        _fail_run(pipeline_run_id, str(exc))
        raise
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


def _fail_run(pipeline_run_id: int, error: str) -> None:
    with PipelineRepository() as repo:
        repo.fail_pipeline_run(pipeline_run_id, error)


def _run_outcome(results: list[dict[str, Any]]) -> str:
    error_statuses = {"failed", "completed_with_errors"}
    return (
        "completed_with_errors"
        if any(result.get("status") in error_statuses for result in results)
        else "successful"
    )
