"""open source queue and pgvector schema

Revision ID: 20260526_0001
Revises:
Create Date: 2026-05-26
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "20260526_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    vector_available = bool(
        bind.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')")
        ).scalar()
    )
    if vector_available:
        op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    embedding_type = "vector(1536)" if vector_available else "DOUBLE PRECISION[]"
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS publications (
            id BIGSERIAL PRIMARY KEY,
            paper_id TEXT NOT NULL UNIQUE,
            doi TEXT,
            title TEXT,
            abstract TEXT,
            year INTEGER,
            source TEXT NOT NULL,
            source_url TEXT,
            open_access_url TEXT,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_publications_doi ON publications (doi)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_publications_year ON publications (year)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id BIGSERIAL PRIMARY KEY,
            run_key TEXT NOT NULL UNIQUE,
            query TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            config JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS stage_runs (
            id BIGSERIAL PRIMARY KEY,
            pipeline_run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            publication_id BIGINT REFERENCES publications(id) ON DELETE CASCADE,
            stage TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            task_id TEXT,
            error TEXT,
            metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (pipeline_run_id, publication_id, stage)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_stage_runs_status ON stage_runs (stage, status)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS artifacts (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT REFERENCES publications(id) ON DELETE CASCADE,
            artifact_type TEXT NOT NULL,
            path TEXT NOT NULL,
            sha256 TEXT,
            bytes BIGINT,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (publication_id, artifact_type, path)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS document_sections (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT REFERENCES publications(id) ON DELETE CASCADE,
            section_type TEXT,
            heading TEXT,
            body TEXT NOT NULL,
            char_start INTEGER,
            char_end INTEGER,
            embedding __EMBEDDING_TYPE__,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """.replace("__EMBEDDING_TYPE__", embedding_type)
    )
    if vector_available:
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_document_sections_embedding "
            "ON document_sections USING ivfflat (embedding vector_cosine_ops)"
        )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_mentions (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT REFERENCES publications(id) ON DELETE CASCADE,
            dataset_name TEXT NOT NULL,
            aliases TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            dataset_role TEXT NOT NULL DEFAULT 'unclear',
            reference_directness TEXT NOT NULL DEFAULT 'unclear',
            evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
            confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
            embedding __EMBEDDING_TYPE__,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """.replace("__EMBEDDING_TYPE__", embedding_type)
    )
    if vector_available:
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_dataset_mentions_embedding "
            "ON dataset_mentions USING ivfflat (embedding vector_cosine_ops)"
        )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS um_datasets (
            id BIGSERIAL PRIMARY KEY,
            um_dataset_id TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            aliases TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            creators TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            doi TEXT,
            url TEXT,
            year INTEGER,
            repository TEXT,
            keywords TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            embedding __EMBEDDING_TYPE__,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """.replace("__EMBEDDING_TYPE__", embedding_type)
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_um_datasets_doi ON um_datasets (doi)")
    if vector_available:
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_um_datasets_embedding "
            "ON um_datasets USING ivfflat (embedding vector_cosine_ops)"
        )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS um_match_decisions (
            id BIGSERIAL PRIMARY KEY,
            dataset_mention_id BIGINT REFERENCES dataset_mentions(id) ON DELETE CASCADE,
            um_dataset_row_id BIGINT REFERENCES um_datasets(id) ON DELETE SET NULL,
            status TEXT NOT NULL,
            match_method TEXT NOT NULL,
            match_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            matched_fields TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            review_required BOOLEAN NOT NULL DEFAULT FALSE,
            reviewer_decision TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS um_match_decisions")
    op.execute("DROP TABLE IF EXISTS um_datasets")
    op.execute("DROP TABLE IF EXISTS dataset_mentions")
    op.execute("DROP TABLE IF EXISTS document_sections")
    op.execute("DROP TABLE IF EXISTS artifacts")
    op.execute("DROP TABLE IF EXISTS stage_runs")
    op.execute("DROP TABLE IF EXISTS pipeline_runs")
    op.execute("DROP TABLE IF EXISTS publications")
