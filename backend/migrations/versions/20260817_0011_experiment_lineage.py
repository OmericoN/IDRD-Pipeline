"""Experiment artifact and rules-v3 detection lineage.

Revision ID: 20260817_0011
Revises: 20260814_0010
Create Date: 2026-08-17
"""

from __future__ import annotations

from alembic import op

revision = "20260817_0011"
down_revision = "20260814_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE discovery_candidates ADD COLUMN download_status TEXT NOT NULL DEFAULT 'not_attempted'"
    )
    op.execute("ALTER TABLE discovery_candidates ADD COLUMN download_failure_category TEXT")
    op.execute("ALTER TABLE discovery_candidates ADD COLUMN download_checked_at TIMESTAMPTZ")
    op.execute(
        """
        ALTER TABLE discovery_candidates
        ADD CONSTRAINT ck_discovery_candidates_download_status
        CHECK (download_status IN ('not_attempted', 'downloaded', 'failed'))
        """
    )
    op.execute(
        """
        CREATE TABLE mention_detection_runs (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            markdown_artifact_id BIGINT NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
            render_sha256 TEXT NOT NULL,
            detector_version TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            candidate_count INTEGER NOT NULL DEFAULT 0,
            metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            UNIQUE (publication_id, markdown_artifact_id, render_sha256, detector_version),
            CHECK (status IN ('running', 'successful', 'failed'))
        )
        """
    )
    op.execute(
        """
        CREATE INDEX idx_mention_detection_runs_active
        ON mention_detection_runs (publication_id, render_sha256, detector_version, status)
        """
    )
    op.execute("ALTER TABLE mention_candidates ADD COLUMN detection_run_id BIGINT REFERENCES mention_detection_runs(id) ON DELETE CASCADE")
    op.execute("ALTER TABLE mention_candidates ADD COLUMN trigger_type TEXT NOT NULL DEFAULT 'legacy_rule'")
    op.execute("ALTER TABLE mention_candidates ADD COLUMN trigger_text TEXT NOT NULL DEFAULT ''")
    op.execute("ALTER TABLE mention_candidates ADD COLUMN triggers JSONB NOT NULL DEFAULT '[]'::jsonb")
    op.execute("ALTER TABLE mention_candidates ADD COLUMN evidence_tier TEXT NOT NULL DEFAULT 'broad'")
    op.execute("ALTER TABLE mention_candidates ADD COLUMN detector_version TEXT NOT NULL DEFAULT 'rules-v1'")
    op.execute("ALTER TABLE mention_candidates ADD COLUMN render_sha256 TEXT")
    op.execute(
        "ALTER TABLE mention_candidates DROP CONSTRAINT IF EXISTS mention_candidates_publication_id_dataset_name_char_start_char_end_key"
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_mention_candidates_detection_window
        ON mention_candidates (detection_run_id, dataset_name, char_start, char_end)
        WHERE detection_run_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_mention_candidates_legacy_window
        ON mention_candidates (publication_id, dataset_name, char_start, char_end)
        WHERE detection_run_id IS NULL
        """
    )
    op.execute(
        """
        ALTER TABLE mention_candidates
        ADD CONSTRAINT ck_mention_candidates_evidence_tier
        CHECK (evidence_tier IN ('strong', 'medium', 'broad'))
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE mention_candidates DROP CONSTRAINT IF EXISTS ck_mention_candidates_evidence_tier")
    op.execute("DROP INDEX IF EXISTS uq_mention_candidates_legacy_window")
    op.execute("DROP INDEX IF EXISTS uq_mention_candidates_detection_window")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS render_sha256")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS detector_version")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS evidence_tier")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS triggers")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS trigger_text")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS trigger_type")
    op.execute("ALTER TABLE mention_candidates DROP COLUMN IF EXISTS detection_run_id")
    op.execute("DROP INDEX IF EXISTS idx_mention_detection_runs_active")
    op.execute("DROP TABLE IF EXISTS mention_detection_runs")
    op.execute(
        "ALTER TABLE mention_candidates ADD CONSTRAINT mention_candidates_publication_id_dataset_name_char_start_char_end_key UNIQUE (publication_id, dataset_name, char_start, char_end)"
    )
    op.execute("ALTER TABLE discovery_candidates DROP CONSTRAINT IF EXISTS ck_discovery_candidates_download_status")
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN IF EXISTS download_checked_at")
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN IF EXISTS download_failure_category")
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN IF EXISTS download_status")
