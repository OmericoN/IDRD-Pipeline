"""Focused persistence methods."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg2.extras

from datasight.domain.schemas import UMDatasetRecord, UMMatchDecision


class UMDatasetRepositoryMixin:
    conn: Any
    cursor: Any

    _UM_DATASET_UPSERT = """
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
        WHERE (
            um_datasets.title, um_datasets.aliases, um_datasets.creators,
            um_datasets.doi, um_datasets.url, um_datasets.year,
            um_datasets.repository, um_datasets.keywords, um_datasets.raw
        ) IS DISTINCT FROM (
            EXCLUDED.title, EXCLUDED.aliases, EXCLUDED.creators,
            EXCLUDED.doi, EXCLUDED.url, EXCLUDED.year,
            EXCLUDED.repository, EXCLUDED.keywords, EXCLUDED.raw
        )
    """

    def upsert_um_datasets(self, records: Iterable[UMDatasetRecord]) -> int:
        record_list = list(records)
        for record in record_list:
            self.cursor.execute(self._UM_DATASET_UPSERT, self._um_dataset_params(record))
        self.conn.commit()
        return len(record_list)

    def sync_um_datasets(self, records: Iterable[UMDatasetRecord]) -> tuple[int, int]:
        """Atomically upsert an authoritative catalog and remove stale records."""
        record_list = list(records)
        if not record_list:
            raise ValueError("Refusing to synchronize an empty UM dataset catalog.")
        try:
            self.cursor.executemany(
                self._UM_DATASET_UPSERT,
                [self._um_dataset_params(record) for record in record_list],
            )
            self.cursor.execute(
                "DELETE FROM um_datasets WHERE NOT (um_dataset_id = ANY(%s))",
                ([record.um_dataset_id for record in record_list],),
            )
            deleted = max(int(getattr(self.cursor, "rowcount", 0)), 0)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return len(record_list), deleted

    @staticmethod
    def _um_dataset_params(record: UMDatasetRecord) -> tuple[Any, ...]:
        return (
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
        )

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

    def get_unmatched_mentions_for_publication(self, publication_row_id: int) -> list[dict[str, Any]]:
        self.cursor.execute(
            """
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
              AND dm.publication_id = %s
            ORDER BY dm.id
            """,
            (publication_row_id,),
        )
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
                    match_score, matched_fields, candidate_um_dataset_ids,
                    review_required, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                """,
                (
                    mention_id,
                    um_dataset_row_id,
                    decision.status,
                    decision.match_method,
                    decision.match_score,
                    decision.matched_fields,
                    decision.candidate_um_dataset_ids,
                    decision.review_required,
                ),
            )
            count += 1
        self.conn.commit()
        return count
