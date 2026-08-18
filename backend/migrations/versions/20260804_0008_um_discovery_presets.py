"""UM discovery previews and run-scoped candidates.

Revision ID: 20260804_0008
Revises: 20260715_0007
Create Date: 2026-08-04
"""

from __future__ import annotations

from alembic import op

revision = "20260804_0008"
down_revision = "20260715_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_previews (
            preview_id TEXT PRIMARY KEY,
            catalog_fingerprint TEXT NOT NULL,
            request JSONB NOT NULL DEFAULT '{}'::jsonb,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_discovery_previews_expires_at
        ON discovery_previews (expires_at)
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_candidates (
            id BIGSERIAL PRIMARY KEY,
            pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            reuse_likelihood DOUBLE PRECISION NOT NULL DEFAULT 0,
            evidence_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            matched_um_dataset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            pipeline_ready BOOLEAN NOT NULL DEFAULT FALSE,
            included BOOLEAN NOT NULL DEFAULT FALSE,
            exclusion_reason TEXT,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (pipeline_run_id, publication_id)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_discovery_candidates_run_score
        ON discovery_candidates (pipeline_run_id, included, pipeline_ready, reuse_likelihood DESC)
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_topic_resolutions (
            catalog_fingerprint TEXT NOT NULL,
            topic_name TEXT NOT NULL,
            topic_id TEXT,
            status TEXT NOT NULL DEFAULT 'unresolved',
            resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_fingerprint, topic_name)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS discovery_topic_resolutions")
    op.execute("DROP INDEX IF EXISTS idx_discovery_candidates_run_score")
    op.execute("DROP TABLE IF EXISTS discovery_candidates")
    op.execute("DROP INDEX IF EXISTS idx_discovery_previews_expires_at")
    op.execute("DROP TABLE IF EXISTS discovery_previews")
