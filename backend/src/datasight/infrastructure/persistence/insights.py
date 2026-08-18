"""Focused persistence methods."""

from __future__ import annotations

from typing import Any


class InsightRepositoryMixin:
    cursor: Any

    def export_insight_rows(self, pipeline_run_id: int | None = None) -> list[dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT
                p.paper_id,
                p.title AS publication_title,
                p.year AS publication_year,
                COALESCE(discovery.discovery_mode, 'unrecorded') AS discovery_mode,
                COALESCE(discovery.discovery_methods, ARRAY[]::TEXT[]) AS discovery_methods,
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
                md.candidate_um_dataset_ids,
                md.review_required,
                ud.um_dataset_id,
                ud.title AS um_dataset_title,
                ud.repository AS um_repository
            FROM dataset_mentions dm
            JOIN publications p ON p.id = dm.publication_id
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(pr.config->>'discovery_mode', 'unrecorded') AS discovery_mode,
                    dc.evidence_reasons AS discovery_methods
                FROM discovery_candidates dc
                JOIN pipeline_runs pr ON pr.id = dc.pipeline_run_id
                WHERE dc.publication_id = p.id
                  AND dc.included
                  AND dc.pipeline_ready
                ORDER BY
                    CASE
                        WHEN %s::BIGINT IS NOT NULL AND dc.pipeline_run_id = %s THEN 0
                        ELSE 1
                    END,
                    dc.updated_at DESC,
                    dc.id DESC
                LIMIT 1
            ) discovery ON TRUE
            LEFT JOIN um_match_decisions md ON md.dataset_mention_id = dm.id
            LEFT JOIN um_datasets ud ON ud.id = md.um_dataset_row_id
            ORDER BY p.paper_id, dm.dataset_name
            """,
            (pipeline_run_id, pipeline_run_id),
        )
        return [dict(row) for row in self.cursor.fetchall()]
