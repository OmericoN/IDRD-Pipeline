"""UM catalog ambiguity candidates.

Revision ID: 20260715_0007
Revises: 20260615_0006
Create Date: 2026-07-15
"""

from __future__ import annotations

from alembic import op

revision = "20260715_0007"
down_revision = "20260615_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE um_match_decisions "
        "ADD COLUMN IF NOT EXISTS candidate_um_dataset_ids TEXT[] "
        "NOT NULL DEFAULT ARRAY[]::TEXT[]"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE um_match_decisions "
        "DROP COLUMN IF EXISTS candidate_um_dataset_ids"
    )
