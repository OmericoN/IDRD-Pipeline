"""PostgreSQL repository for the canonical Alembic schema."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg2
import psycopg2.extras

from idrd.config import POSTGRES_DSN
from idrd.models.results import ConversionResult, DownloadResult, RenderResult
from idrd.pipeline.schemas import DatasetMention, MentionCandidate, UMDatasetRecord, UMMatchDecision


class PipelineRepository:
    def __init__(self, dsn: str = POSTGRES_DSN):
        self.conn: Any = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self.cursor: Any = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def healthcheck(self) -> dict[str, Any]:
        self.cursor.execute("SELECT current_database() AS database, current_user AS user")
        connection = dict(self.cursor.fetchone() or {})

        self.cursor.execute("SELECT to_regclass('public.publications') AS relation")
        publications_exists = bool((self.cursor.fetchone() or {}).get("relation"))
        self.cursor.execute("SELECT to_regclass('public.mention_candidates') AS relation")
        candidates_exists = bool((self.cursor.fetchone() or {}).get("relation"))

        self.cursor.execute("SELECT to_regclass('public.alembic_version') AS relation")
        alembic_exists = bool((self.cursor.fetchone() or {}).get("relation"))
        alembic_revision = None
        if alembic_exists:
            self.cursor.execute("SELECT version_num FROM alembic_version")
            row = self.cursor.fetchone()
            alembic_revision = row["version_num"] if row else None

        self.cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') AS installed")
        pgvector_installed = bool((self.cursor.fetchone() or {}).get("installed"))

        return {
            "connection": connection,
            "publications_table": publications_exists,
            "mention_candidates_table": candidates_exists,
            "alembic_table": alembic_exists,
            "alembic_revision": alembic_revision,
            "pgvector_extension": pgvector_installed,
            "ready": publications_exists and candidates_exists,
            "vector_search_ready": publications_exists and pgvector_installed,
        }

    def create_pipeline_run(self, query: str, config: dict[str, Any] | None = None) -> int:
        run_key = f"run-{uuid4()}"
        self.cursor.execute(
            """
            INSERT INTO pipeline_runs (run_key, query, status, config)
            VALUES (%s, %s, 'running', %s)
            RETURNING id
            """,
            (run_key, query, psycopg2.extras.Json(config or {})),
        )
        row = self.cursor.fetchone()
        if not row:
            raise RuntimeError("Failed to create pipeline run")
        self.conn.commit()
        return int(row["id"])

    def update_pipeline_run_task_id(self, pipeline_run_id: int, task_id: str | None) -> None:
        self.cursor.execute(
            "UPDATE pipeline_runs SET celery_task_id = %s, updated_at = now() WHERE id = %s",
            (task_id, pipeline_run_id),
        )
        self.conn.commit()

    def record_stage_result(
        self,
        stage: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        pipeline_run_id: int | None = None,
        error: str | None = None,
    ) -> None:
        if pipeline_run_id is None:
            return
        self.cursor.execute(
            """
            INSERT INTO stage_runs (
                pipeline_run_id, stage, status, attempt_count, error, metrics,
                started_at, finished_at, updated_at
            )
            VALUES (%s, %s, %s, 1, %s, %s, now(), now(), now())
            """,
            (
                pipeline_run_id,
                stage,
                status,
                error,
                psycopg2.extras.Json(metrics or {}),
            ),
        )
        self.conn.commit()

    def finish_pipeline_run(self, pipeline_run_id: int | None, status: str) -> None:
        if pipeline_run_id is None:
            return
        self.cursor.execute(
            "UPDATE pipeline_runs SET status = %s, finished_at = now(), updated_at = now() WHERE id = %s",
            (status, pipeline_run_id),
        )
        self.conn.commit()

    def fail_pipeline_run(self, pipeline_run_id: int | None, error: str) -> None:
        if pipeline_run_id is None:
            return
        self.cursor.execute(
            """
            UPDATE pipeline_runs
            SET status = 'failed', error = %s, finished_at = now(), updated_at = now()
            WHERE id = %s
            """,
            (error, pipeline_run_id),
        )
        self.conn.commit()

    def list_pipeline_runs(self, limit: int = 25) -> list[dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT
                pr.id,
                pr.run_key,
                pr.query,
                pr.status,
                pr.config,
                pr.celery_task_id,
                pr.error,
                pr.created_at,
                pr.updated_at,
                pr.finished_at
            FROM pipeline_runs pr
            ORDER BY pr.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        runs = [dict(row) for row in self.cursor.fetchall()]
        for run in runs:
            run["stages"] = self.list_stage_runs(int(run["id"]))
        return runs

    def get_pipeline_run(self, pipeline_run_id: int) -> dict[str, Any] | None:
        self.cursor.execute(
            """
            SELECT
                id,
                run_key,
                query,
                status,
                config,
                celery_task_id,
                error,
                created_at,
                updated_at,
                finished_at
            FROM pipeline_runs
            WHERE id = %s
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone()
        if not row:
            return None
        run = dict(row)
        run["stages"] = self.list_stage_runs(pipeline_run_id)
        return run

    def list_stage_runs(self, pipeline_run_id: int) -> list[dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT
                id,
                stage,
                status,
                attempt_count,
                task_id,
                error,
                metrics,
                started_at,
                finished_at,
                created_at,
                updated_at
            FROM stage_runs
            WHERE pipeline_run_id = %s
            ORDER BY id
            """,
            (pipeline_run_id,),
        )
        return [dict(row) for row in self.cursor.fetchall()]

    def active_run_count(self) -> int:
        self.cursor.execute(
            "SELECT COUNT(*) AS count FROM pipeline_runs WHERE status IN ('queued', 'running', 'started')"
        )
        row = self.cursor.fetchone() or {}
        return int(row.get("count") or 0)

    def reset_database(self) -> list[str]:
        tables = [
            "stage_runs",
            "um_match_decisions",
            "mention_candidates",
            "dataset_mentions",
            "document_sections",
            "artifacts",
            "um_datasets",
            "publications",
            "pipeline_runs",
        ]
        self.cursor.execute(f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE")
        self.conn.commit()
        return tables

    def upsert_publications(self, papers: Iterable[dict[str, Any]], source: str) -> int:
        count = 0
        for paper in papers:
            paper_id = paper.get("paperId") or paper.get("id")
            if not paper_id:
                continue
            external_ids = paper.get("externalIds") or {}
            open_access = paper.get("openAccessPdf") or {}
            self.cursor.execute(
                """
                INSERT INTO publications (
                    paper_id, doi, title, abstract, year, source, source_url,
                    open_access_url, raw, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (paper_id) DO UPDATE SET
                    doi = EXCLUDED.doi,
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    year = EXCLUDED.year,
                    source = EXCLUDED.source,
                    source_url = EXCLUDED.source_url,
                    open_access_url = EXCLUDED.open_access_url,
                    raw = EXCLUDED.raw,
                    updated_at = now()
                """,
                (
                    paper_id,
                    external_ids.get("DOI"),
                    paper.get("title"),
                    paper.get("abstract"),
                    paper.get("year"),
                    source,
                    paper.get("url"),
                    open_access.get("url") if isinstance(open_access, dict) else None,
                    psycopg2.extras.Json(paper),
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def get_papers_needing_download(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT p.paper_id AS "paperId", p.title, p.open_access_url AS url
            FROM publications p
            LEFT JOIN artifacts a
              ON a.publication_id = p.id AND a.artifact_type = 'pdf'
            WHERE p.open_access_url IS NOT NULL
              AND p.open_access_url != ''
              AND a.id IS NULL
            ORDER BY p.id
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def get_papers_needing_conversion(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT p.paper_id AS "paperId", p.title, pdf.path AS pdf_path
            FROM publications p
            JOIN artifacts pdf
              ON pdf.publication_id = p.id AND pdf.artifact_type = 'pdf'
            LEFT JOIN artifacts xml
              ON xml.publication_id = p.id AND xml.artifact_type = 'tei_xml'
            WHERE xml.id IS NULL
            ORDER BY p.id
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def get_papers_needing_rendering(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT p.paper_id AS "paperId", p.title, xml.path AS xml_path
            FROM publications p
            JOIN artifacts xml
              ON xml.publication_id = p.id AND xml.artifact_type = 'tei_xml'
            LEFT JOIN artifacts md
              ON md.publication_id = p.id AND md.artifact_type = 'markdown'
            WHERE md.id IS NULL
            ORDER BY p.id
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def persist_download_results(self, results: Iterable[DownloadResult]) -> int:
        return self._persist_artifacts("pdf", results, "filepath", "file_size_bytes")

    def persist_conversion_results(self, results: Iterable[ConversionResult]) -> int:
        return self._persist_artifacts("tei_xml", results, "xml_path", "xml_size_bytes")

    def persist_render_results(self, results: Iterable[RenderResult]) -> int:
        return self._persist_artifacts("markdown", results, "md_path", None)

    def _persist_artifacts(
        self,
        artifact_type: str,
        results: Iterable[Any],
        path_attr: str,
        size_attr: str | None,
    ) -> int:
        count = 0
        for result in results:
            if not result.success:
                continue
            path = getattr(result, path_attr, None)
            if not path:
                continue
            self.cursor.execute("SELECT id FROM publications WHERE paper_id = %s", (result.paper_id,))
            row = self.cursor.fetchone()
            if not row:
                continue
            bytes_value = getattr(result, size_attr, None) if size_attr else _path_size(path)
            self.cursor.execute(
                """
                INSERT INTO artifacts (publication_id, artifact_type, path, bytes, metadata)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (publication_id, artifact_type, path) DO UPDATE SET
                    bytes = EXCLUDED.bytes,
                    metadata = EXCLUDED.metadata
                """,
                (
                    row["id"],
                    artifact_type,
                    str(path),
                    bytes_value,
                    psycopg2.extras.Json({"message": result.message}),
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def get_markdown_artifacts_without_candidates(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT p.id AS publication_row_id, p.paper_id, md.path
            FROM publications p
            JOIN artifacts md
              ON md.publication_id = p.id AND md.artifact_type = 'markdown'
            LEFT JOIN mention_candidates c
              ON c.publication_id = p.id
            WHERE c.id IS NULL
            ORDER BY p.id
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def upsert_mention_candidates(self, candidates: Iterable[MentionCandidate]) -> int:
        count = 0
        for candidate in candidates:
            self.cursor.execute(
                "SELECT id FROM publications WHERE paper_id = %s",
                (candidate.publication_id,),
            )
            row = self.cursor.fetchone()
            if not row:
                continue
            self.cursor.execute(
                """
                INSERT INTO mention_candidates (
                    publication_id, dataset_name, evidence_text, section_heading,
                    standardized_section, char_start, char_end, score, source, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (publication_id, dataset_name, char_start, char_end)
                DO UPDATE SET
                    evidence_text = EXCLUDED.evidence_text,
                    section_heading = EXCLUDED.section_heading,
                    standardized_section = EXCLUDED.standardized_section,
                    score = EXCLUDED.score,
                    source = EXCLUDED.source,
                    updated_at = now()
                """,
                (
                    row["id"],
                    candidate.dataset_name,
                    candidate.evidence_text,
                    candidate.section_heading,
                    candidate.standardized_section,
                    candidate.char_start,
                    candidate.char_end,
                    candidate.score,
                    candidate.source,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def get_unprocessed_candidates(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT
                c.id AS candidate_id,
                p.paper_id AS publication_id,
                c.dataset_name,
                c.evidence_text,
                c.section_heading,
                c.standardized_section,
                c.char_start,
                c.char_end,
                c.score,
                c.source
            FROM mention_candidates c
            JOIN publications p ON p.id = c.publication_id
            WHERE c.promoted_mention_id IS NULL
            ORDER BY c.id
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def persist_dataset_mentions(self, mentions: Iterable[DatasetMention], candidate_ids: Iterable[int]) -> int:
        count = 0
        for mention, candidate_id in zip(mentions, candidate_ids, strict=False):
            self.cursor.execute("SELECT id FROM publications WHERE paper_id = %s", (mention.publication_id,))
            publication = self.cursor.fetchone()
            if not publication:
                continue
            publication_id = publication["id"]
            char_start = mention.provenance.char_start

            self.cursor.execute(
                """
                SELECT id
                FROM dataset_mentions
                WHERE publication_id = %s
                  AND dataset_name = %s
                  AND provenance->>'char_start' IS NOT DISTINCT FROM %s
                """,
                (
                    publication_id,
                    mention.dataset_name,
                    str(char_start) if char_start is not None else None,
                ),
            )
            existing = self.cursor.fetchone()
            if existing:
                mention_id = existing["id"]
                self.cursor.execute(
                    """
                    UPDATE dataset_mentions
                    SET aliases = %s,
                        dataset_role = %s,
                        reference_directness = %s,
                        evidence = %s,
                        metadata = %s,
                        provenance = %s,
                        confidence = %s,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        mention.aliases,
                        mention.dataset_role,
                        mention.reference_directness,
                        psycopg2.extras.Json(mention.evidence.model_dump(mode="json")),
                        psycopg2.extras.Json(mention.metadata.model_dump(mode="json")),
                        psycopg2.extras.Json(mention.provenance.model_dump(mode="json")),
                        mention.provenance.confidence,
                        mention_id,
                    ),
                )
            else:
                self.cursor.execute(
                    """
                    INSERT INTO dataset_mentions (
                        publication_id, dataset_name, aliases, dataset_role,
                        reference_directness, evidence, metadata, provenance, confidence
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        publication_id,
                        mention.dataset_name,
                        mention.aliases,
                        mention.dataset_role,
                        mention.reference_directness,
                        psycopg2.extras.Json(mention.evidence.model_dump(mode="json")),
                        psycopg2.extras.Json(mention.metadata.model_dump(mode="json")),
                        psycopg2.extras.Json(mention.provenance.model_dump(mode="json")),
                        mention.provenance.confidence,
                    ),
                )
                inserted = self.cursor.fetchone()
                if not inserted:
                    raise RuntimeError("Failed to persist dataset mention")
                mention_id = inserted["id"]

            self.cursor.execute(
                "UPDATE mention_candidates SET promoted_mention_id = %s, updated_at = now() WHERE id = %s",
                (mention_id, candidate_id),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_um_datasets(self, records: Iterable[UMDatasetRecord]) -> int:
        count = 0
        for record in records:
            self.cursor.execute(
                """
                INSERT INTO um_datasets (
                    um_dataset_id, title, aliases, creators, doi, url, year,
                    repository, keywords, raw, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (um_dataset_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    aliases = EXCLUDED.aliases,
                    creators = EXCLUDED.creators,
                    doi = EXCLUDED.doi,
                    url = EXCLUDED.url,
                    year = EXCLUDED.year,
                    repository = EXCLUDED.repository,
                    keywords = EXCLUDED.keywords,
                    raw = EXCLUDED.raw,
                    updated_at = now()
                """,
                (
                    record.um_dataset_id,
                    record.title,
                    record.aliases,
                    record.creators,
                    record.doi,
                    record.url,
                    record.year,
                    record.repository,
                    record.keywords,
                    psycopg2.extras.Json(record.raw),
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def list_um_dataset_records(self) -> list[UMDatasetRecord]:
        self.cursor.execute(
            """
            SELECT um_dataset_id, title, aliases, creators, doi, url, year, repository, keywords, raw
            FROM um_datasets
            ORDER BY id
            """
        )
        return [UMDatasetRecord.model_validate(dict(row)) for row in self.cursor.fetchall()]

    def get_unmatched_mentions(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT
                dm.id AS mention_id,
                p.paper_id AS publication_id,
                dm.dataset_name,
                dm.aliases,
                dm.dataset_role,
                dm.reference_directness,
                dm.evidence,
                dm.metadata,
                dm.provenance
            FROM dataset_mentions dm
            JOIN publications p ON p.id = dm.publication_id
            LEFT JOIN um_match_decisions md ON md.dataset_mention_id = dm.id
            WHERE md.id IS NULL
            ORDER BY dm.id
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def persist_match_decisions(self, decisions: Iterable[UMMatchDecision]) -> int:
        count = 0
        for decision in decisions:
            mention_id = int(decision.mention_id) if decision.mention_id else None
            um_dataset_row_id = None
            if decision.um_dataset_id:
                self.cursor.execute(
                    "SELECT id FROM um_datasets WHERE um_dataset_id = %s",
                    (decision.um_dataset_id,),
                )
                row = self.cursor.fetchone()
                um_dataset_row_id = row["id"] if row else None
            self.cursor.execute(
                """
                INSERT INTO um_match_decisions (
                    dataset_mention_id, um_dataset_row_id, status, match_method,
                    match_score, matched_fields, review_required, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                """,
                (
                    mention_id,
                    um_dataset_row_id,
                    decision.status,
                    decision.match_method,
                    decision.match_score,
                    decision.matched_fields,
                    decision.review_required,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def export_insight_rows(self) -> list[dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT
                p.paper_id,
                p.title AS publication_title,
                p.year AS publication_year,
                dm.dataset_name,
                dm.dataset_role,
                dm.reference_directness,
                dm.confidence,
                dm.evidence,
                dm.metadata,
                md.status AS match_status,
                md.match_method,
                md.match_score,
                md.matched_fields,
                md.review_required,
                ud.um_dataset_id,
                ud.title AS um_dataset_title,
                ud.repository AS um_repository
            FROM dataset_mentions dm
            JOIN publications p ON p.id = dm.publication_id
            LEFT JOIN um_match_decisions md ON md.dataset_mention_id = dm.id
            LEFT JOIN um_datasets ud ON ud.id = md.um_dataset_row_id
            ORDER BY p.paper_id, dm.dataset_name
            """
        )
        return [dict(row) for row in self.cursor.fetchall()]

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "PipelineRepository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.conn.rollback()
        self.close()


def _path_size(path: str | Path) -> int | None:
    try:
        return Path(path).stat().st_size
    except OSError:
        return None
