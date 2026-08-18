"""Recall-safe, atomic PDF downloads used by pipeline services."""

from __future__ import annotations

import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

import requests

from datasight.config import (
    DOWNLOAD_BACKOFF_SEC,
    DOWNLOAD_CHUNK_SIZE_BYTES,
    DOWNLOAD_DELAY_SEC,
    DOWNLOAD_MAX_BYTES,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_TIMEOUT_SEC,
    PDF_DIR,
)
from datasight.domain.results import DownloadResult
from datasight.infrastructure.ingestion.file_integrity import sha256_file

logger = logging.getLogger(__name__)
PDF_DOWNLOADER_VERSION = "pdf-downloader-v2"
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


class DownloadValidationError(ValueError):
    def __init__(self, category: str, message: str):
        super().__init__(message)
        self.category = category


class PDFDownloader:
    """Download PDFs to temporary files and publish only validated artifacts."""

    def __init__(
        self,
        output_dir: str | Path | None = None,
        delay: float = DOWNLOAD_DELAY_SEC,
        max_bytes: int = DOWNLOAD_MAX_BYTES,
        max_retries: int = DOWNLOAD_MAX_RETRIES,
        backoff: float = DOWNLOAD_BACKOFF_SEC,
    ):
        self.output_dir = Path(output_dir) if output_dir else PDF_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.max_bytes = max_bytes
        self.max_retries = max(1, max_retries)
        self.backoff = max(0.0, backoff)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }

    def generate_filename(self, paper_id: str) -> str:
        safe_id = re.sub(r'[<>:"/\\|?*]', "", paper_id)
        return f"{safe_id}.pdf"

    def is_valid_pdf(self, filepath: Path) -> bool:
        try:
            self._validate_pdf(filepath)
            return True
        except (OSError, DownloadValidationError):
            return False

    def _validate_pdf(self, filepath: Path) -> None:
        size = filepath.stat().st_size
        if size <= 0:
            raise DownloadValidationError("empty_body", "Downloaded response was empty")
        if size > self.max_bytes:
            raise DownloadValidationError("oversized", f"PDF exceeds {self.max_bytes} bytes")
        with filepath.open("rb") as handle:
            header = handle.read(min(1024, size))
            if b"%PDF-" not in header:
                lowered = header.lstrip().lower()
                category = "html_body" if lowered.startswith((b"<!doctype html", b"<html")) else "invalid_pdf"
                raise DownloadValidationError(category, "Response does not contain PDF magic")
            handle.seek(max(0, size - 4096))
            trailer = handle.read()
        if b"%%EOF" not in trailer:
            raise DownloadValidationError("truncated_pdf", "PDF end-of-file marker is missing")

    def _failure(
        self,
        paper_id: str,
        url: str | None,
        category: str,
        message: str,
        error: str | None = None,
    ) -> DownloadResult:
        return DownloadResult(
            paper_id=paper_id,
            success=False,
            message=message,
            error=error or message,
            url=url,
            producer_version=PDF_DOWNLOADER_VERSION,
            failure_category=category,
            warnings=[],
            quality_metrics={},
        )

    def download_paper(
        self,
        paper_id: str,
        url: str | None,
        title: str | None = None,
        overwrite: bool = False,
    ) -> DownloadResult:
        del title
        if not url:
            return self._failure(paper_id, url, "missing_url", "No URL provided")

        filepath = self.output_dir / self.generate_filename(paper_id)
        if filepath.exists() and not overwrite and self.is_valid_pdf(filepath):
            return DownloadResult(
                paper_id=paper_id,
                success=True,
                message=f"Reused validated PDF: {filepath.name}",
                filepath=filepath,
                file_size_bytes=filepath.stat().st_size,
                url=url,
                sha256=sha256_file(filepath),
                producer_version=PDF_DOWNLOADER_VERSION,
                warnings=[],
                quality_metrics={"cache_reused": True},
            )

        last_category = "request_error"
        last_error = "Download failed"
        for attempt in range(self.max_retries):
            temp_path: Path | None = None
            response: Any = None
            try:
                response = requests.get(url, headers=self.headers, timeout=DOWNLOAD_TIMEOUT_SEC, stream=True)
                status = int(response.status_code)
                if status in RETRYABLE_STATUS_CODES:
                    raise requests.exceptions.HTTPError(f"Retryable HTTP {status}", response=response)
                if status >= 400:
                    return self._failure(paper_id, url, "http_client_error", f"HTTP {status} while downloading", str(status))

                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > self.max_bytes:
                    return self._failure(paper_id, url, "oversized", f"Response exceeds {self.max_bytes} bytes")

                warnings: list[str] = []
                content_type = response.headers.get("Content-Type", "").lower()
                if "pdf" not in content_type and "application/octet-stream" not in content_type:
                    warnings.append(f"Mislabeled Content-Type accepted after magic validation: {content_type or 'missing'}")

                with tempfile.NamedTemporaryFile(
                    mode="wb", prefix=f".{filepath.name}.", suffix=".part", dir=self.output_dir, delete=False
                ) as handle:
                    temp_path = Path(handle.name)
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        if downloaded > self.max_bytes:
                            raise DownloadValidationError("oversized", f"PDF exceeds {self.max_bytes} bytes")
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())

                self._validate_pdf(temp_path)
                digest = sha256_file(temp_path)
                os.replace(temp_path, filepath)
                temp_path = None
                return DownloadResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Downloaded and validated: {filepath.name}",
                    filepath=filepath,
                    file_size_bytes=filepath.stat().st_size,
                    url=url,
                    sha256=digest,
                    producer_version=PDF_DOWNLOADER_VERSION,
                    warnings=warnings,
                    quality_metrics={"cache_reused": False, "content_type": content_type, "attempts": attempt + 1},
                )
            except DownloadValidationError as exc:
                last_category, last_error = exc.category, str(exc)
                if exc.category in {"oversized", "html_body"}:
                    return self._failure(paper_id, url, exc.category, str(exc))
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                last_category, last_error = "timeout_or_connection", str(exc)
            except requests.exceptions.HTTPError as exc:
                status = getattr(exc.response, "status_code", None)
                last_category, last_error = "retryable_http", f"HTTP {status}"
            except requests.exceptions.RequestException as exc:
                last_category, last_error = "request_error", str(exc)
            except (OSError, ValueError) as exc:
                last_category, last_error = "stream_error", str(exc)
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
                close = getattr(response, "close", None)
                if callable(close):
                    close()

            if attempt < self.max_retries - 1:
                time.sleep(self.backoff * (2**attempt))

        return self._failure(
            paper_id,
            url,
            last_category,
            f"Download failed after {self.max_retries} attempts: {last_error}",
            last_error,
        )

    def download_papers(
        self,
        papers: list[dict[str, Any]],
        paper_id_key: str = "paperId",
        url_key: str = "url",
        title_key: str = "title",
        overwrite: bool = False,
    ) -> list[DownloadResult]:
        results: list[DownloadResult] = []
        for paper in papers:
            results.append(
                self.download_paper(
                    paper_id=paper.get(paper_id_key, "unknown"),
                    url=paper.get(url_key),
                    title=paper.get(title_key),
                    overwrite=overwrite,
                )
            )
            if self.delay > 0:
                time.sleep(self.delay)
        logger.info("Downloaded %s/%s PDFs", sum(result.success for result in results), len(results))
        return results
