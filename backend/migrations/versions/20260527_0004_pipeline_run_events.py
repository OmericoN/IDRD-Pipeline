"""pipeline run structured events

Revision ID: 20260527_0004
Revises: 20260527_0003
Create Date: 2026-05-27
"""

from __future__ import annotations

from alembic import op

revision = "20260527_0004"
down_revision = "20260527_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_run_events (
            id BIGSERIAL PRIMARY KEY,
            pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            stage TEXT,
            level TEXT NOT NULL DEFAULT 'info',
            message TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_run_events_run_created
        ON pipeline_run_events (pipeline_run_id, created_at, id)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pipeline_run_events")
