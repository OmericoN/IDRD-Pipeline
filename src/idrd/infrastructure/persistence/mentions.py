"""Focused persistence methods."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg2.extras

from idrd.domain.schemas import DatasetMention, MentionCandidate


class MentionRepositoryMixin:
    conn: Any
    cursor: Any

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
