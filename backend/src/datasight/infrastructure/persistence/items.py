"""Persistence helpers for high-throughput per-publication pipeline items."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg2.extras

from datasight.domain.stages import PipelineStage
from datasight.infrastructure.persistence.events import event_level, event_message


ITEM_STAGES: tuple[PipelineStage, ...] = (
    PipelineStage.DOWNLOAD_PDF,
    PipelineStage.GROBID_CONVERT,
    PipelineStage.RENDER_DOCUMENT,
    PipelineStage.DETECT_MENTIONS,
    PipelineStage.EXTRACT_FEATURES,
    PipelineStage.MATCH_UM_DATASET,
)

TERMINAL_STATUSES = {"successful", "failed", "skipped"}
ACTIVE_STATUSES = {"queued", "running", "started"}


class PipelineItemRepositoryMixin:
    conn: Any
    cursor: Any

    def _insert_pipeline_run_event(
        self,
        pipeline_run_id: int,
        stage: str | None,
        level: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError

    def create_pipeline_items(self, pipeline_run_id: int, paper_ids: Iterable[str]) -> list[int]:
        item_ids: list[int] = []
        for paper_id in paper_ids:
            self.cursor.execute("SELECT id FROM publications WHERE paper_id = %s", (paper_id,))
            publication = self.cursor.fetchone()
            if not publication:
                continue
            self.cursor.execute(
                """
                INSERT INTO pipeline_items (pipeline_run_id, publication_id, status, updated_at)
                VALUES (%s, %s, 'pending', now())
                ON CONFLICT (pipeline_run_id, publication_id) DO UPDATE SET
                    updated_at = now()
                RETURNING id
                """,
                (pipeline_run_id, publication["id"]),
            )
            item = self.cursor.fetchone()
            if not item:
                continue
            item_id = int(item["id"])
            item_ids.append(item_id)
            for stage in ITEM_STAGES:
                self.cursor.execute(
                    """
                    INSERT INTO pipeline_item_stages (pipeline_item_id, stage, status)
                    VALUES (%s, %s, 'pending')
                    ON CONFLICT (pipeline_item_id, stage) DO NOTHING
                    """,
                    (item_id, stage.value),
                )
        self.conn.commit()
        return item_ids

    def queue_item_stage_for_run(self, pipeline_run_id: int, stage: PipelineStage | str) -> int:
        stage_value = PipelineStage(stage).value
        self.cursor.execute(
            """
            UPDATE pipeline_item_stages pis
            SET status = 'queued',
                updated_at = now()
            FROM pipeline_items pi
            WHERE pi.id = pis.pipeline_item_id
              AND pi.pipeline_run_id = %s
              AND pis.stage = %s
              AND pis.status = 'pending'
            RETURNING pis.pipeline_item_id
            """,
            (pipeline_run_id, stage_value),
        )
        queued = len(self.cursor.fetchall())
        self.conn.commit()
        if queued:
            self.refresh_stage_aggregate(pipeline_run_id, stage_value)
        return queued

    def get_pipeline_item_context(self, item_id: int) -> dict[str, Any] | None:
        self.cursor.execute(
            """
            SELECT
                pi.id AS item_id,
                pi.pipeline_run_id,
                pi.status AS item_status,
                p.id AS publication_row_id,
                p.paper_id,
                p.title,
                p.open_access_url,
                pdf.path AS pdf_path,
                xml.path AS xml_path,
                md.path AS markdown_path
            FROM pipeline_items pi
            JOIN publications p ON p.id = pi.publication_id
            LEFT JOIN artifacts pdf
              ON pdf.publication_id = p.id AND pdf.artifact_type = 'pdf'
            LEFT JOIN artifacts xml
              ON xml.publication_id = p.id AND xml.artifact_type = 'tei_xml'
            LEFT JOIN artifacts md
              ON md.publication_id = p.id AND md.artifact_type = 'markdown'
            WHERE pi.id = %s
            """,
            (item_id,),
        )
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def start_item_stage(self, item_id: int, stage: PipelineStage | str, task_id: str | None = None) -> bool:
        stage_value = PipelineStage(stage).value
        self.cursor.execute(
            """
            UPDATE pipeline_item_stages
            SET status = 'running',
                attempt_count = attempt_count + 1,
                task_id = %s,
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            WHERE pipeline_item_id = %s
              AND stage = %s
              AND status = 'queued'
            RETURNING id
            """,
            (task_id, item_id, stage_value),
        )
        started = self.cursor.fetchone() is not None
        if started:
            self.cursor.execute(
                "UPDATE pipeline_items SET status = 'running', updated_at = now() WHERE id = %s",
                (item_id,),
            )
            self.conn.commit()
            self.refresh_stage_aggregate_for_item(item_id, stage_value)
        return started

    def claim_queued_item_stages(
        self,
        pipeline_run_id: int,
        stage: PipelineStage | str,
        limit: int,
        task_id: str | None = None,
    ) -> list[int]:
        stage_value = PipelineStage(stage).value
        self.cursor.execute(
            """
            WITH selected AS (
                SELECT pis.id
                FROM pipeline_item_stages pis
                JOIN pipeline_items pi ON pi.id = pis.pipeline_item_id
                WHERE pi.pipeline_run_id = %s
                  AND pis.stage = %s
                  AND pis.status = 'queued'
                ORDER BY pis.id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE pipeline_item_stages pis
            SET status = 'running',
                attempt_count = attempt_count + 1,
                task_id = %s,
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            FROM selected
            WHERE pis.id = selected.id
            RETURNING pis.pipeline_item_id
            """,
            (pipeline_run_id, stage_value, limit, task_id),
        )
        item_ids = [int(row["pipeline_item_id"]) for row in self.cursor.fetchall()]
        if item_ids:
            self.cursor.execute(
                """
                UPDATE pipeline_items
                SET status = 'running',
                    updated_at = now()
                WHERE id = ANY(%s)
                """,
                (item_ids,),
            )
        self.conn.commit()
        if item_ids:
            self.refresh_stage_aggregate(pipeline_run_id, stage_value)
        return item_ids

    def finish_item_stage(
        self,
        item_id: int,
        stage: PipelineStage | str,
        status: str,
        metrics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        stage_value = PipelineStage(stage).value
        self.cursor.execute(
            """
            UPDATE pipeline_item_stages
            SET status = %s,
                metrics = %s,
                error = %s,
                finished_at = now(),
                updated_at = now()
            WHERE pipeline_item_id = %s AND stage = %s
            """,
            (status, psycopg2.extras.Json(metrics or {}), error, item_id, stage_value),
        )
        if status != "successful":
            self._skip_downstream_item_stages(item_id, stage_value)
            self.cursor.execute(
                "UPDATE pipeline_items SET status = 'failed', updated_at = now() WHERE id = %s",
                (item_id,),
            )
        elif next_stage := self._next_stage(stage_value):
            self.cursor.execute(
                """
                UPDATE pipeline_item_stages
                SET status = 'queued',
                    updated_at = now()
                WHERE pipeline_item_id = %s
                  AND stage = %s
                  AND status = 'pending'
                """,
                (item_id, next_stage.value),
            )
        elif stage_value == PipelineStage.MATCH_UM_DATASET.value:
            self.cursor.execute(
                "UPDATE pipeline_items SET status = 'successful', updated_at = now() WHERE id = %s",
                (item_id,),
            )
        self.conn.commit()
        self.refresh_stage_aggregate_for_item(item_id, stage_value)
        if status == "successful" and (next_stage := self._next_stage(stage_value)):
            self.refresh_stage_aggregate_for_item(item_id, next_stage.value)
        if status != "successful":
            for downstream in self._downstream_stages(stage_value):
                self.refresh_stage_aggregate_for_item(item_id, downstream.value)

    def all_item_stages_terminal(self, pipeline_run_id: int) -> bool:
        self.cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM pipeline_item_stages pis
            JOIN pipeline_items pi ON pi.id = pis.pipeline_item_id
            WHERE pi.pipeline_run_id = %s
              AND pis.status NOT IN ('successful', 'failed', 'skipped')
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone() or {}
        return int(row.get("count") or 0) == 0

    def get_queued_item_stage_counts(self, pipeline_run_id: int) -> dict[str, int]:
        self.cursor.execute(
            """
            SELECT pis.stage, COUNT(*) AS count
            FROM pipeline_item_stages pis
            JOIN pipeline_items pi ON pi.id = pis.pipeline_item_id
            WHERE pi.pipeline_run_id = %s
              AND pis.status = 'queued'
            GROUP BY pis.stage
            """,
            (pipeline_run_id,),
        )
        return {str(row["stage"]): int(row["count"]) for row in self.cursor.fetchall()}

    def get_running_item_stage_count(self, pipeline_run_id: int) -> int:
        self.cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM pipeline_item_stages pis
            JOIN pipeline_items pi ON pi.id = pis.pipeline_item_id
            WHERE pi.pipeline_run_id = %s
              AND pis.status IN ('running', 'started')
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone() or {}
        return int(row.get("count") or 0)

    def high_throughput_outcome(self, pipeline_run_id: int) -> str:
        self.cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE pis.status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE pis.status = 'skipped') AS skipped
            FROM pipeline_item_stages pis
            JOIN pipeline_items pi ON pi.id = pis.pipeline_item_id
            WHERE pi.pipeline_run_id = %s
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone() or {}
        failed = int(row.get("failed") or 0)
        skipped = int(row.get("skipped") or 0)
        return "completed_with_errors" if failed or skipped else "successful"

    def try_start_run_level_stage(self, pipeline_run_id: int, stage: PipelineStage | str) -> bool:
        stage_value = PipelineStage(stage).value
        self.cursor.execute(
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s)) AS locked",
            (pipeline_run_id, stage_value),
        )
        lock_row = self.cursor.fetchone() or {}
        if not lock_row.get("locked"):
            self.conn.commit()
            return False

        self.cursor.execute(
            """
            SELECT id FROM stage_runs
            WHERE pipeline_run_id = %s AND publication_id IS NULL AND stage = %s
            """,
            (pipeline_run_id, stage_value),
        )
        if self.cursor.fetchone():
            self.conn.commit()
            return False

        self.cursor.execute(
            """
            INSERT INTO stage_runs (
                pipeline_run_id, stage, status, attempt_count, metrics,
                started_at, updated_at
            )
            VALUES (%s, %s, 'running', 1, '{}'::jsonb, now(), now())
            """,
            (pipeline_run_id, stage_value),
        )
        self._insert_pipeline_run_event(
            pipeline_run_id=pipeline_run_id,
            stage=stage_value,
            level="info",
            message=f"{stage_value}: running.",
            payload={},
        )
        self.conn.commit()
        return True

    def refresh_stage_aggregate_for_item(self, item_id: int, stage: PipelineStage | str) -> None:
        self.cursor.execute(
            "SELECT pipeline_run_id FROM pipeline_items WHERE id = %s",
            (item_id,),
        )
        row = self.cursor.fetchone()
        if not row:
            return
        self.refresh_stage_aggregate(int(row["pipeline_run_id"]), stage)

    def refresh_stage_aggregate(self, pipeline_run_id: int, stage: PipelineStage | str) -> None:
        stage_value = PipelineStage(stage).value
        self.cursor.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE pis.status = 'successful') AS successful,
                COUNT(*) FILTER (WHERE pis.status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE pis.status = 'skipped') AS skipped,
                COUNT(*) FILTER (WHERE pis.status = 'queued') AS queued,
                COUNT(*) FILTER (WHERE pis.status IN ('running', 'started')) AS running,
                COUNT(*) FILTER (WHERE pis.status = 'pending') AS pending
            FROM pipeline_item_stages pis
            JOIN pipeline_items pi ON pi.id = pis.pipeline_item_id
            WHERE pi.pipeline_run_id = %s AND pis.stage = %s
            """,
            (pipeline_run_id, stage_value),
        )
        counts = dict(self.cursor.fetchone() or {})
        total = int(counts.get("total") or 0)
        if total == 0:
            return

        successful = int(counts.get("successful") or 0)
        failed = int(counts.get("failed") or 0)
        skipped = int(counts.get("skipped") or 0)
        queued = int(counts.get("queued") or 0)
        running = int(counts.get("running") or 0)
        pending = int(counts.get("pending") or 0)
        terminal = successful + failed + skipped

        if running:
            status = "running"
        elif queued:
            status = "queued"
        elif pending:
            status = "pending"
        elif failed:
            status = "failed"
        elif successful:
            status = "successful"
        else:
            status = "skipped"

        metrics = {
            "total": total,
            "successful": successful,
            "failed": failed,
            "skipped": skipped,
            "queued": queued,
            "running": running,
            "active": queued + running,
            "pending": pending,
        }
        finished_at_expr = "now()" if terminal == total and not queued and not running and not pending else "NULL"
        self.cursor.execute(
            """
            SELECT id FROM stage_runs
            WHERE pipeline_run_id = %s AND publication_id IS NULL AND stage = %s
            ORDER BY id
            LIMIT 1
            """,
            (pipeline_run_id, stage_value),
        )
        existing = self.cursor.fetchone()
        if existing:
            self.cursor.execute(
                f"""
                UPDATE stage_runs
                SET status = %s,
                    metrics = %s,
                    started_at = COALESCE(started_at, now()),
                    finished_at = {finished_at_expr},
                    updated_at = now()
                WHERE id = %s
                """,
                (status, psycopg2.extras.Json(metrics), existing["id"]),
            )
        else:
            self.cursor.execute(
                f"""
                INSERT INTO stage_runs (
                    pipeline_run_id, stage, status, attempt_count, metrics,
                    started_at, finished_at, updated_at
                )
                VALUES (%s, %s, %s, 1, %s, now(), {finished_at_expr}, now())
                """,
                (pipeline_run_id, stage_value, status, psycopg2.extras.Json(metrics)),
            )

        self._insert_pipeline_run_event(
            pipeline_run_id=pipeline_run_id,
            stage=stage_value,
            level=event_level(status),
            message=event_message(stage_value, status, {"count": terminal, **metrics}, None),
            payload=metrics,
        )
        self.conn.commit()

    def _skip_downstream_item_stages(self, item_id: int, failed_stage: str) -> None:
        for stage in self._downstream_stages(failed_stage):
            self.cursor.execute(
                """
                UPDATE pipeline_item_stages
                SET status = 'skipped',
                    metrics = %s,
                    finished_at = now(),
                    updated_at = now()
                WHERE pipeline_item_id = %s
                  AND stage = %s
                  AND status NOT IN ('successful', 'failed', 'skipped')
                """,
                (
                    psycopg2.extras.Json({"reason": f"Skipped after {failed_stage} did not succeed."}),
                    item_id,
                    stage.value,
                ),
            )

    def _downstream_stages(self, stage: str) -> tuple[PipelineStage, ...]:
        stage_order = [item.value for item in ITEM_STAGES]
        if stage not in stage_order:
            return ()
        index = stage_order.index(stage)
        return ITEM_STAGES[index + 1 :]

    def _next_stage(self, stage: str) -> PipelineStage | None:
        downstream = self._downstream_stages(stage)
        return downstream[0] if downstream else None
