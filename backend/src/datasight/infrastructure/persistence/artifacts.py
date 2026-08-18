"""Focused persistence methods."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg2.extras

from datasight.domain.results import ConversionResult, DownloadResult, RenderResult
from datasight.infrastructure.ingestion.file_integrity import sha256_file
from datasight.infrastructure.persistence.files import path_size


class ArtifactRepositoryMixin:
    conn: Any
    cursor: Any

    def persist_download_results(self, results: Iterable[DownloadResult]) -> int:
        materialized = list(results)
        count = self._persist_artifacts("pdf", materialized, "filepath", "file_size_bytes")
        for result in materialized:
            self.cursor.execute("SELECT id FROM publications WHERE paper_id = %s", (result.paper_id,))
            publication = self.cursor.fetchone()
            if publication:
                self.cursor.execute(
                    """
                    UPDATE discovery_candidates
                    SET download_status = %s,
                        download_failure_category = %s,
                        download_checked_at = now(),
                        updated_at = now()
                    WHERE publication_id = %s
                    """,
                    (
                        "downloaded" if result.success else "failed",
                        result.failure_category,
                        publication["id"],
                    ),
                )
        self.conn.commit()
        return count

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
            digest = getattr(result, "sha256", None) or sha256_file(path)
            metadata = {
                "message": result.message,
                "source_sha256": getattr(result, "source_sha256", None),
                "producer_version": getattr(result, "producer_version", None),
                "warnings": getattr(result, "warnings", None) or [],
                "quality_metrics": getattr(result, "quality_metrics", None) or {},
            }
            profile = getattr(result, "profile", None)
            if profile:
                metadata["profile"] = profile
            self.cursor.execute(
                """
                INSERT INTO artifacts (publication_id, artifact_type, path, sha256, bytes, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (publication_id, artifact_type, path) DO UPDATE SET
                    sha256 = EXCLUDED.sha256,
                    bytes = EXCLUDED.bytes,
                    metadata = EXCLUDED.metadata
                """,
                (
                    row["id"],
                    artifact_type,
                    str(path),
                    digest,
                    bytes_value,
                    psycopg2.extras.Json(metadata),
                ),
            )
            count += 1
        self.conn.commit()
        return count
