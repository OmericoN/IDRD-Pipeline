"""Database-backed high-throughput pipeline execution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Any

from datasight.application import pipeline_services as services
from datasight.config import (
    DEFAULT_UM_DATASETS_PATH,
    HIGH_THROUGHPUT_MAX_BATCHES_PER_DISPATCH,
    HIGH_THROUGHPUT_STAGE_BATCH_SIZE,
)
from datasight.domain.stages import PipelineStage
from datasight.infrastructure.persistence.items import ITEM_STAGES
from datasight.infrastructure.persistence.repository import PipelineRepository

STAGE_QUEUES: dict[PipelineStage, str] = {
    PipelineStage.DOWNLOAD_PDF: "download",
    PipelineStage.GROBID_CONVERT: "grobid",
    PipelineStage.RENDER_DOCUMENT: "processing",
    PipelineStage.DETECT_MENTIONS: "processing",
    PipelineStage.EXTRACT_FEATURES: "processing",
    PipelineStage.MATCH_UM_DATASET: "matching",
}


@dataclass(frozen=True)
class StageBatch:
    stage: PipelineStage
    queue: str
    batch_size: int


@dataclass(frozen=True)
class DispatchPlan:
    batches: tuple[StageBatch, ...]
    finalize: bool = False


def bootstrap_high_throughput_run(
    query: str,
    limit: int,
    um_datasets_path: str | None = DEFAULT_UM_DATASETS_PATH,
    overwrite: bool = False,
    open_access_only: bool = True,
    topic_ids: str | list[str] | None = None,
    keyword_terms: str | list[str] | None = None,
    mesh_terms: str | list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    use_um_profile: bool = True,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    if pipeline_run_id is None:
        raise ValueError("pipeline_run_id is required for high-throughput bootstrap.")
    if um_datasets_path:
        services.import_um_datasets(um_datasets_path)

    result = services.discover_publications(
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
    )
    payload = result.get("payload", {})
    raw_paper_ids = payload.get("paper_ids", []) if isinstance(payload, dict) else []
    paper_ids = [paper_id for paper_id in raw_paper_ids if isinstance(paper_id, str)]
    with PipelineRepository() as repo:
        item_ids = repo.create_pipeline_items(pipeline_run_id, paper_ids)
        queued = (
            repo.queue_item_stage_for_run(pipeline_run_id, PipelineStage.DOWNLOAD_PDF)
            if item_ids
            else 0
        )
    return {
        "pipeline_run_id": pipeline_run_id,
        "items": len(item_ids),
        "queued": queued,
        "overwrite": overwrite,
        "status": "queued" if queued else "skipped",
    }


def build_dispatch_plan(
    pipeline_run_id: int,
    batch_size: int = HIGH_THROUGHPUT_STAGE_BATCH_SIZE,
    max_batches: int = HIGH_THROUGHPUT_MAX_BATCHES_PER_DISPATCH,
) -> DispatchPlan:
    with PipelineRepository() as repo:
        if repo.all_item_stages_terminal(pipeline_run_id):
            started = repo.try_start_run_level_stage(
                pipeline_run_id, PipelineStage.EXPORT_INSIGHTS
            )
            return DispatchPlan(batches=(), finalize=started)

        queued_counts = repo.get_queued_item_stage_counts(pipeline_run_id)

    batches: list[StageBatch] = []
    safe_batch_size = max(1, batch_size)
    safe_max_batches = max(1, max_batches)
    for stage in ITEM_STAGES:
        queued = queued_counts.get(stage.value, 0)
        if queued <= 0:
            continue
        batch_count = min(safe_max_batches, ceil(queued / safe_batch_size))
        batches.extend(
            StageBatch(
                stage=stage, queue=STAGE_QUEUES[stage], batch_size=safe_batch_size
            )
            for _ in range(batch_count)
        )
    return DispatchPlan(batches=tuple(batches))


def process_high_throughput_stage(
    pipeline_run_id: int,
    stage: PipelineStage | str,
    batch_size: int = HIGH_THROUGHPUT_STAGE_BATCH_SIZE,
    overwrite: bool = False,
    task_id: str | None = None,
) -> dict[str, Any]:
    stage_value = PipelineStage(stage)
    with PipelineRepository() as repo:
        item_ids = repo.claim_queued_item_stages(
            pipeline_run_id=pipeline_run_id,
            stage=stage_value,
            limit=max(1, batch_size),
            task_id=task_id,
        )

    results: list[dict[str, Any]] = []
    for item_id in item_ids:
        try:
            results.append(
                _process_claimed_item_stage(item_id, stage_value, overwrite, task_id)
            )
        except (
            Exception
        ) as exc:  # pragma: no cover - defensive boundary around per-item work
            message = f"{stage_value.value} item processing error: {exc}"
            with PipelineRepository() as repo:
                repo.finish_item_stage(
                    item_id, stage_value, "failed", {"message": message}, str(exc)
                )
            results.append(
                {
                    "stage": stage_value.value,
                    "status": "failed",
                    "item_id": item_id,
                    "message": message,
                    "payload": {"error": str(exc)},
                }
            )

    return {
        "pipeline_run_id": pipeline_run_id,
        "stage": stage_value.value,
        "claimed": len(item_ids),
        "successful": sum(result["status"] == "successful" for result in results),
        "failed": sum(result["status"] == "failed" for result in results),
        "skipped": sum(result["status"] == "skipped" for result in results),
        "results": results,
    }


def finalize_high_throughput_run(
    output_path: str, pipeline_run_id: int | None = None
) -> dict[str, Any]:
    if pipeline_run_id is None:
        raise ValueError(
            "pipeline_run_id is required for high-throughput finalization."
        )
    result = services.export_insights_csv(output_path, pipeline_run_id=pipeline_run_id)
    with PipelineRepository() as repo:
        status = repo.high_throughput_outcome(pipeline_run_id)
        repo.finish_pipeline_run(pipeline_run_id, status)
    return {"pipeline_run_id": pipeline_run_id, "status": status, "export": result}


def _process_claimed_item_stage(
    item_id: int,
    stage: PipelineStage,
    overwrite: bool,
    task_id: str | None,
) -> dict[str, Any]:
    if stage == PipelineStage.DOWNLOAD_PDF:
        return services.download_pipeline_item(
            item_id, overwrite=overwrite, task_id=task_id, claimed=True
        )
    if stage == PipelineStage.GROBID_CONVERT:
        return services.grobid_convert_pipeline_item(
            item_id,
            overwrite=overwrite,
            delete_pdf=False,
            task_id=task_id,
            claimed=True,
        )
    if stage == PipelineStage.RENDER_DOCUMENT:
        return services.render_pipeline_item(
            item_id, overwrite=overwrite, task_id=task_id, claimed=True
        )
    if stage == PipelineStage.DETECT_MENTIONS:
        return services.detect_mentions_pipeline_item(
            item_id, task_id=task_id, claimed=True
        )
    if stage == PipelineStage.EXTRACT_FEATURES:
        return services.extract_features_pipeline_item(
            item_id, task_id=task_id, claimed=True
        )
    if stage == PipelineStage.MATCH_UM_DATASET:
        return services.match_um_dataset_pipeline_item(
            item_id, task_id=task_id, claimed=True
        )
    raise ValueError(f"Unsupported high-throughput item stage: {stage.value}")
