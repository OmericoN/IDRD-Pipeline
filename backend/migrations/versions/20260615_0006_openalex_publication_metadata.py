"""openalex publication metadata

Revision ID: 20260615_0006
Revises: 20260601_0005
Create Date: 2026-06-15
"""

from __future__ import annotations

from alembic import op

revision = "20260615_0006"
down_revision = "20260601_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS publication_date DATE")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS language TEXT")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS publication_type TEXT")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS oa_status TEXT")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS cited_by_count INTEGER")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS is_retracted BOOLEAN")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS has_fulltext BOOLEAN")
    op.execute("ALTER TABLE publications ADD COLUMN IF NOT EXISTS primary_source_name TEXT")
    op.execute("CREATE INDEX IF NOT EXISTS idx_publications_openalex_type ON publications (publication_type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_publications_oa_status ON publications (oa_status)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publication_openalex_affiliations (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            auth_index INTEGER,
            openalex_author_id TEXT,
            openalex_author TEXT,
            openalex_orcid TEXT,
            openalex_is_corresponding BOOLEAN,
            openalex_inst_id TEXT,
            openalex_inst_name TEXT,
            openalex_inst_ror TEXT,
            openalex_inst_country_code TEXT,
            openalex_inst_type TEXT,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publication_openalex_topics (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            domain TEXT,
            domain_id TEXT,
            field TEXT,
            field_id TEXT,
            subfield TEXT,
            subfield_id TEXT,
            topic TEXT,
            topic_id TEXT,
            score DOUBLE PRECISION,
            topic_primary BOOLEAN,
            topic_index INTEGER,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_publication_openalex_topics_topic_id ON publication_openalex_topics (topic_id)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publication_openalex_keywords (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            keyword TEXT NOT NULL,
            score DOUBLE PRECISION,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publication_openalex_concepts (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            level TEXT,
            concept TEXT NOT NULL,
            score DOUBLE PRECISION,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publication_openalex_mesh (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            mesh TEXT NOT NULL,
            qualifier TEXT,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publication_openalex_related_works (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            related_work_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (publication_id, related_work_id)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS publication_openalex_related_works")
    op.execute("DROP TABLE IF EXISTS publication_openalex_mesh")
    op.execute("DROP TABLE IF EXISTS publication_openalex_concepts")
    op.execute("DROP TABLE IF EXISTS publication_openalex_keywords")
    op.execute("DROP INDEX IF EXISTS idx_publication_openalex_topics_topic_id")
    op.execute("DROP TABLE IF EXISTS publication_openalex_topics")
    op.execute("DROP TABLE IF EXISTS publication_openalex_affiliations")
    op.execute("DROP INDEX IF EXISTS idx_publications_oa_status")
    op.execute("DROP INDEX IF EXISTS idx_publications_openalex_type")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS primary_source_name")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS has_fulltext")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS is_retracted")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS cited_by_count")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS oa_status")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS publication_type")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS language")
    op.execute("ALTER TABLE publications DROP COLUMN IF EXISTS publication_date")

