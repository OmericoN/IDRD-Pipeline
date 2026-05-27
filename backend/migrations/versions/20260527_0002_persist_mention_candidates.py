"""persist mention candidates

Revision ID: 20260527_0002
Revises: 20260526_0001
Create Date: 2026-05-27
"""

from __future__ import annotations

from alembic import op

revision = "20260527_0002"
down_revision = "20260526_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS mention_candidates (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            dataset_name TEXT NOT NULL,
            evidence_text TEXT NOT NULL,
            section_heading TEXT,
            standardized_section TEXT,
            char_start INTEGER NOT NULL,
            char_end INTEGER NOT NULL,
            score DOUBLE PRECISION NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'rule',
            promoted_mention_id BIGINT REFERENCES dataset_mentions(id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (publication_id, dataset_name, char_start, char_end)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mention_candidates_unprocessed
        ON mention_candidates (promoted_mention_id)
        WHERE promoted_mention_id IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dataset_mentions_publication
        ON dataset_mentions (publication_id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_um_match_decisions_status
        ON um_match_decisions (status, review_required)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_um_match_decisions_status")
    op.execute("DROP INDEX IF EXISTS idx_dataset_mentions_publication")
    op.execute("DROP INDEX IF EXISTS idx_mention_candidates_unprocessed")
    op.execute("DROP TABLE IF EXISTS mention_candidates")
