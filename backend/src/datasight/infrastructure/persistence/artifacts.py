"""Focused persistence methods."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg2.extras

from datasight.infrastructure.persistence.files import path_size
from datasight.domain.results import ConversionResult, DownloadResult, RenderResult


class ArtifactRepositoryMixin:
    conn: Any
    cursor: Any

    def persist_download_results(self, results: Iterable[DownloadResult]) -> int:
        return self._persist_artifacts("pdf", results, "filepath", "file_size_bytes")

    def persist_conversion_results(self, results: Iterable[ConversionResult]) -> int:
        return self._persist_artifacts("tei_xml", results, "xml_path", "xml_size_bytes")

    def persist_render_results(self, results: Iterable[RenderResult]) -> int:
        return self._persist_artifacts("markdown", results, "md_path", None)

    def _persist_artifacts(
        self,
        artifact_type: str,
        results: Iterable[Any],
        path_attr: str,
        size_attr: str | None,
    ) -> int:
        count = 0
        for result in results:
            if not result.success:
                continue
            path = getattr(result, path_attr, None)
            if not path:
                continue
            self.cursor.execute("SELECT id FROM publications WHERE paper_id = %s", (result.paper_id,))
            row = self.cursor.fetchone()
            if not row:
                continue
            bytes_value = getattr(result, size_attr, None) if size_attr else path_size(path)
            self.cursor.execute(
                """
                INSERT INTO artifacts (publication_id, artifact_type, path, bytes, metadata)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (publication_id, artifact_type, path) DO UPDATE SET
                    bytes = EXCLUDED.bytes,
                    metadata = EXCLUDED.metadata
                """,
                (
                    row["id"],
                    artifact_type,
                    str(path),
                    bytes_value,
                    psycopg2.extras.Json({"message": result.message}),
                ),
            )
            count += 1
        self.conn.commit()
        return count
