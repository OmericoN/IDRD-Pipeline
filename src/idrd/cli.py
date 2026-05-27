"""CLI for the queue-backed IDRD pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import redis

from idrd.config import CELERY_BROKER_URL
from idrd.models.results import ConversionResult, DownloadResult
from idrd.pipeline.stages import PipelineStage, stage_values


class IDRDPipeline:
    """Small compatibility helpers for result classification tests."""

    @staticmethod
    def _download_result_status(result: DownloadResult) -> str:
        if not result.success:
            return "failed"
        return "skipped" if result.message.lower().startswith("already exists:") else "successful"

    @staticmethod
    def _convert_result_status(result: ConversionResult) -> str:
        if not result.success:
            return "failed"
        return "skipped" if "already converted" in result.message.lower() else "successful"

    @staticmethod
    def _render_result_status(success: bool, message: str) -> str:
        if not success:
            return "failed"
        return "skipped" if message.lower().startswith("already exists:") else "successful"

    @staticmethod
    def _summarize_status_counts(statuses: list[str]) -> dict[str, int]:
        return {
            "successful": sum(1 for status in statuses if status == "successful"),
            "failed": sum(1 for status in statuses if status == "failed"),
            "skipped": sum(1 for status in statuses if status == "skipped"),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="uv run idrd",
        description="IDRD Pipeline - queue-backed dataset mention pipeline",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    enqueue = subcommands.add_parser("enqueue", help="Enqueue one Celery stage")
    _add_stage_arguments(enqueue)

    run = subcommands.add_parser("run-local", help="Run one stage synchronously")
    _add_stage_arguments(run)

    import_um = subcommands.add_parser("import-um-datasets", help="Import UM dataset metadata JSON/CSV")
    import_um.add_argument("--path", required=True)

    run_all = subcommands.add_parser("run-all", help="Run or enqueue the full pipeline")
    run_all.add_argument("--query", required=True)
    run_all.add_argument("--limit", type=int, default=100)
    run_all.add_argument("--um-datasets", required=True)
    run_all.add_argument("--output", required=True)
    run_all.add_argument("--mode", choices=["guided", "local", "enqueue"], default="guided")
    run_all.add_argument("--fields-of-study")
    run_all.add_argument("--all-access", action="store_true")
    run_all.add_argument("--overwrite", action="store_true")

    subcommands.add_parser("worker-command", help="Print the Celery worker command")
    subcommands.add_parser("stages", help="Print canonical stage order")
    subcommands.add_parser("doctor", help="Check database schema readiness")

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "worker-command":
        print("uv run celery -A idrd.pipeline.celery_app:celery_app worker --loglevel=INFO --pool=solo")
        return

    if args.command == "stages":
        print(json.dumps(stage_values(), indent=2))
        return

    if args.command == "doctor":
        print(json.dumps(_doctor(), indent=2, default=str))
        return

    if args.command == "import-um-datasets":
        from idrd.pipeline import services

        print(json.dumps(services.import_um_datasets(args.path), indent=2, default=str))
        return

    if args.command == "run-all":
        print(json.dumps(_run_all(args), indent=2, default=str))
        return

    if args.command == "enqueue":
        result = _enqueue(args)
    elif args.command == "run-local":
        result = _run_local(args)
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(result, indent=2, default=str))


def _add_stage_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("stage", choices=stage_values())
    parser.add_argument("--query")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--fields-of-study")
    parser.add_argument("--all-access", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--delete-pdf", action="store_true")
    parser.add_argument("--payload-json", help="Deprecated: only used for export rows")
    parser.add_argument("--output", help="Output path for export_insights")


def _doctor() -> dict[str, Any]:
    from idrd.pipeline.orchestrator import celery_worker_available
    from idrd.storage.repository import PipelineRepository

    with PipelineRepository() as repo:
        health = repo.healthcheck()
    health["redis_ready"] = _redis_ready()
    health["worker_ready"] = celery_worker_available()
    return health


def _enqueue(args: argparse.Namespace) -> dict[str, Any]:
    from idrd.pipeline import tasks

    task = _task_for_stage(args.stage, tasks)
    async_result = task.delay(*_stage_args(args), **_stage_kwargs(args))
    return {"task_id": async_result.id, "stage": args.stage, "status": "queued"}


def _run_local(args: argparse.Namespace) -> dict[str, Any]:
    from idrd.pipeline import services

    stage = PipelineStage(args.stage)
    if stage == PipelineStage.DISCOVER:
        return services.discover_publications(
            query=_require(args.query, "--query"),
            limit=args.limit or 100,
            open_access_only=not args.all_access,
            fields_of_study=args.fields_of_study,
        )
    if stage == PipelineStage.DOWNLOAD_PDF:
        return services.download_pdf_batch(limit=args.limit, overwrite=args.overwrite)
    if stage == PipelineStage.GROBID_CONVERT:
        return services.grobid_convert_batch(limit=args.limit, overwrite=args.overwrite, delete_pdf=args.delete_pdf)
    if stage == PipelineStage.RENDER_DOCUMENT:
        return services.render_document_batch(limit=args.limit, overwrite=args.overwrite)
    if stage == PipelineStage.DETECT_MENTIONS:
        return services.detect_mentions_batch(limit=args.limit)
    if stage == PipelineStage.EXTRACT_FEATURES:
        return services.extract_features_from_candidates(limit=args.limit)
    if stage == PipelineStage.MATCH_UM_DATASET:
        return services.match_um_dataset_batch(limit=args.limit)
    if stage == PipelineStage.EXPORT_INSIGHTS:
        return services.export_insights_csv(_require(args.output, "--output"), _payload_rows(args) or None)
    raise ValueError(f"Unsupported stage: {args.stage}")


def _run_all(args: argparse.Namespace) -> dict[str, Any]:
    from idrd.pipeline import orchestrator

    health = _doctor()
    if not health["ready"]:
        raise SystemExit("Database is not ready. Run migrations first: uv run alembic upgrade head")

    mode = args.mode
    if mode == "guided":
        mode = _prompt_run_mode(worker_ready=health["worker_ready"], redis_ready=health["redis_ready"])

    if mode == "enqueue":
        if not health["redis_ready"]:
            raise SystemExit("Redis is not reachable; cannot enqueue the full run.")
        if not health["worker_ready"]:
            raise SystemExit("No Celery worker responded; start a worker before enqueueing run-all.")
        return orchestrator.enqueue_run_all(
            query=args.query,
            limit=args.limit,
            output=args.output,
            um_datasets_path=args.um_datasets,
            overwrite=args.overwrite,
            open_access_only=not args.all_access,
            fields_of_study=args.fields_of_study,
        )

    return orchestrator.run_all_local(
        query=args.query,
        limit=args.limit,
        output=args.output,
        um_datasets_path=args.um_datasets,
        overwrite=args.overwrite,
        open_access_only=not args.all_access,
        fields_of_study=args.fields_of_study,
    )


def _prompt_run_mode(worker_ready: bool, redis_ready: bool) -> str:
    if not sys.stdin.isatty():
        return "enqueue" if worker_ready and redis_ready else "local"
    default = "enqueue" if worker_ready and redis_ready else "local"
    prompt = f"Run mode [{default}]? Type 'enqueue' or 'local': "
    answer = input(prompt).strip().lower()
    return answer if answer in {"enqueue", "local"} else default


def _redis_ready() -> bool:
    try:
        client = redis.Redis.from_url(CELERY_BROKER_URL, socket_connect_timeout=1, socket_timeout=1)
        return bool(client.ping())
    except Exception:
        return False


def _task_for_stage(stage_value: str, tasks_module: Any) -> Any:
    return {
        PipelineStage.DISCOVER: tasks_module.discover_publications,
        PipelineStage.DOWNLOAD_PDF: tasks_module.download_pdf,
        PipelineStage.GROBID_CONVERT: tasks_module.grobid_convert,
        PipelineStage.RENDER_DOCUMENT: tasks_module.render_document,
        PipelineStage.DETECT_MENTIONS: tasks_module.detect_mentions,
        PipelineStage.EXTRACT_FEATURES: tasks_module.extract_features,
        PipelineStage.MATCH_UM_DATASET: tasks_module.match_um_dataset,
        PipelineStage.EXPORT_INSIGHTS: tasks_module.export_insights,
    }[PipelineStage(stage_value)]


def _stage_args(args: argparse.Namespace) -> list[Any]:
    stage = PipelineStage(args.stage)
    if stage == PipelineStage.DISCOVER:
        return [_require(args.query, "--query")]
    if stage == PipelineStage.EXPORT_INSIGHTS:
        return [_require(args.output, "--output")]
    return []


def _stage_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    stage = PipelineStage(args.stage)
    if stage == PipelineStage.DISCOVER:
        return {
            "limit": args.limit or 100,
            "open_access_only": not args.all_access,
            "fields_of_study": args.fields_of_study,
        }
    if stage in {PipelineStage.DOWNLOAD_PDF, PipelineStage.RENDER_DOCUMENT}:
        return {"limit": args.limit, "overwrite": args.overwrite}
    if stage == PipelineStage.GROBID_CONVERT:
        return {"limit": args.limit, "overwrite": args.overwrite, "delete_pdf": args.delete_pdf}
    if stage in {PipelineStage.DETECT_MENTIONS, PipelineStage.EXTRACT_FEATURES, PipelineStage.MATCH_UM_DATASET}:
        return {"limit": args.limit}
    if stage == PipelineStage.EXPORT_INSIGHTS:
        return {"rows": _payload_rows(args) or None}
    return {}


def _payload_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    if not args.payload_json:
        return []
    payload = json.loads(args.payload_json)
    if isinstance(payload, dict):
        return payload.get("rows") or [payload]
    return payload


def _require(value: str | None, flag: str) -> str:
    if not value:
        raise SystemExit(f"{flag} is required for this command")
    return value


if __name__ == "__main__":
    main()
