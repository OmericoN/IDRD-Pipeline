"""Focused persistence methods."""

from __future__ import annotations

from typing import Any


class InsightRepositoryMixin:
    cursor: Any

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
                md.candidate_um_dataset_ids,
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
