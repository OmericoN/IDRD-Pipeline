"""Focused persistence methods."""

from __future__ import annotations

from typing import Any, Self

import psycopg2
import psycopg2.extras

from idrd.config import POSTGRES_DSN


class PipelineRepositoryBase:
    def __init__(self, dsn: str = POSTGRES_DSN):
        self.conn: Any = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self.cursor: Any = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def healthcheck(self) -> dict[str, Any]:
        self.cursor.execute("SELECT current_database() AS database, current_user AS user")
        connection = dict(self.cursor.fetchone() or {})

        self.cursor.execute("SELECT to_regclass('public.publications') AS relation")
        publications_exists = bool((self.cursor.fetchone() or {}).get("relation"))
        self.cursor.execute("SELECT to_regclass('public.mention_candidates') AS relation")
        candidates_exists = bool((self.cursor.fetchone() or {}).get("relation"))

        self.cursor.execute("SELECT to_regclass('public.alembic_version') AS relation")
        alembic_exists = bool((self.cursor.fetchone() or {}).get("relation"))
        alembic_revision = None
        if alembic_exists:
            self.cursor.execute("SELECT version_num FROM alembic_version")
            row = self.cursor.fetchone()
            alembic_revision = row["version_num"] if row else None

        self.cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') AS installed")
        pgvector_installed = bool((self.cursor.fetchone() or {}).get("installed"))

        return {
            "connection": connection,
            "publications_table": publications_exists,
            "mention_candidates_table": candidates_exists,
            "alembic_table": alembic_exists,
            "alembic_revision": alembic_revision,
            "pgvector_extension": pgvector_installed,
            "ready": publications_exists and candidates_exists,
            "vector_search_ready": publications_exists and pgvector_installed,
        }

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.conn.rollback()
        self.close()
