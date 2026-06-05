"""high throughput pipeline item stages

Revision ID: 20260601_0005
Revises: 20260527_0004
Create Date: 2026-06-01
"""

from __future__ import annotations

from alembic import op

revision = "20260601_0005"
down_revision = "20260527_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_items (
            id BIGSERIAL PRIMARY KEY,
            pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (pipeline_run_id, publication_id)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_item_stages (
            id BIGSERIAL PRIMARY KEY,
            pipeline_item_id BIGINT NOT NULL REFERENCES pipeline_items(id) ON DELETE CASCADE,
            stage TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            task_id TEXT,
            error TEXT,
            metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (pipeline_item_id, stage)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_items_run_status
        ON pipeline_items (pipeline_run_id, status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_item_stages_stage_status
        ON pipeline_item_stages (stage, status)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_pipeline_item_stages_stage_status")
    op.execute("DROP INDEX IF EXISTS idx_pipeline_items_run_status")
    op.execute("DROP TABLE IF EXISTS pipeline_item_stages")
    op.execute("DROP TABLE IF EXISTS pipeline_items")
