"""drop stale legacy mention uniqueness constraint

Revision ID: 20260824_0012
Revises: 20260817_0011
Create Date: 2026-08-24
"""

from __future__ import annotations

from alembic import op

revision = "20260824_0012"
down_revision = "20260817_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # PostgreSQL generated and truncated this name when the original table was
    # created. Migration 0011 tried to drop the untruncated spelling, so the
    # legacy constraint remained alongside the run-scoped partial indexes.
    op.execute(
        """
        ALTER TABLE mention_candidates
        DROP CONSTRAINT IF EXISTS mention_candidates_publication_id_dataset_name_char_start_c_key
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE mention_candidates
        ADD CONSTRAINT mention_candidates_publication_id_dataset_name_char_start_c_key
        UNIQUE (publication_id, dataset_name, char_start, char_end)
        """
    )
