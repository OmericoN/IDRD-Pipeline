"""Adaptive discovery funnel candidate contract.

Revision ID: 20260814_0009
Revises: 20260804_0008
Create Date: 2026-08-14
"""

from __future__ import annotations

from alembic import op

revision = "20260814_0009"
down_revision = "20260804_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_discovery_candidates_run_score")
    op.execute(
        "ALTER TABLE discovery_candidates RENAME COLUMN reuse_likelihood TO candidate_strength"
    )
    op.execute(
        "ALTER TABLE discovery_candidates ADD COLUMN evidence_tier TEXT NOT NULL DEFAULT 'expanded'"
    )
    op.execute(
        """
        UPDATE discovery_candidates
        SET evidence_tier = CASE
            WHEN evidence_reasons && ARRAY[
                'dataset_link', 'dataset_citation', 'dataset_links', 'dataset_citations'
            ]::TEXT[] THEN 'direct'
            WHEN evidence_reasons && ARRAY[
                'identifier_mention', 'title_mention', 'identifier_mentions', 'title_mentions'
            ]::TEXT[] THEN 'exact'
            ELSE 'expanded'
        END
        """
    )
    op.execute(
        """
        ALTER TABLE discovery_candidates
        ADD CONSTRAINT ck_discovery_candidates_evidence_tier
        CHECK (evidence_tier IN ('direct', 'exact', 'expanded'))
        """
    )
    op.execute(
        """
        CREATE INDEX idx_discovery_candidates_run_score
        ON discovery_candidates (
            pipeline_run_id, included, pipeline_ready, evidence_tier, candidate_strength DESC
        )
        """
    )
    # Previews are short-lived provider snapshots and v1 payloads are deliberately incompatible.
    op.execute("DELETE FROM discovery_previews")


def downgrade() -> None:
    op.execute("DELETE FROM discovery_previews")
    op.execute("DROP INDEX IF EXISTS idx_discovery_candidates_run_score")
    op.execute(
        "ALTER TABLE discovery_candidates DROP CONSTRAINT IF EXISTS ck_discovery_candidates_evidence_tier"
    )
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN evidence_tier")
    op.execute(
        "ALTER TABLE discovery_candidates RENAME COLUMN candidate_strength TO reuse_likelihood"
    )
    op.execute(
        """
        CREATE INDEX idx_discovery_candidates_run_score
        ON discovery_candidates (pipeline_run_id, included, pipeline_ready, reuse_likelihood DESC)
        """
    )
