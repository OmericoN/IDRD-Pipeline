"""Focused persistence methods."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg2.extras


class PublicationRepositoryMixin:
    conn: Any
    cursor: Any

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
