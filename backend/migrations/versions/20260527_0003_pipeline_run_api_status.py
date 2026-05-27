"""pipeline run api status fields

Revision ID: 20260527_0003
Revises: 20260527_0002
Create Date: 2026-05-27
"""

from __future__ import annotations

from alembic import op

revision = "20260527_0003"
down_revision = "20260527_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS celery_task_id TEXT")
    op.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS error TEXT")
    op.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ")


def downgrade() -> None:
    op.execute("ALTER TABLE pipeline_runs DROP COLUMN IF EXISTS finished_at")
    op.execute("ALTER TABLE pipeline_runs DROP COLUMN IF EXISTS error")
    op.execute("ALTER TABLE pipeline_runs DROP COLUMN IF EXISTS celery_task_id")
