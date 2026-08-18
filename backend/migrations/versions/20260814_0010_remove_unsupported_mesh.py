"""Remove unsupported MeSH metadata.

Revision ID: 20260814_0010
Revises: 20260814_0009
Create Date: 2026-08-14
"""

from __future__ import annotations

from alembic import op

revision = "20260814_0010"
down_revision = "20260814_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS publication_openalex_mesh")


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE publication_openalex_mesh (
            id BIGSERIAL PRIMARY KEY,
            publication_id BIGINT NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
            mesh TEXT NOT NULL,
            qualifier TEXT,
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
