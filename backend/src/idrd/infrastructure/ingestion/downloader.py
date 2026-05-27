"""PDF download helpers used by pipeline services."""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import requests

from idrd.config import DOWNLOAD_CHUNK_SIZE_BYTES, DOWNLOAD_DELAY_SEC, DOWNLOAD_TIMEOUT_SEC, PDF_DIR
from idrd.domain.results import DownloadResult

logger = logging.getLogger(__name__)


class PDFDownloader:
    """Download open-access PDFs and return structured result objects."""

    def __init__(self, output_dir: str | Path | None = None, delay: float = DOWNLOAD_DELAY_SEC):
        self.output_dir = Path(output_dir) if output_dir else PDF_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.max_retries = 3
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
            with filepath.open("rb") as handle:
                return handle.read(4) == b"%PDF"
        except OSError:
            return False

    def download_paper(
        self,
        paper_id: str,
        url: str | None,
        title: str | None = None,
        overwrite: bool = False,
    ) -> DownloadResult:
        if not url:
            return DownloadResult(
                paper_id=paper_id,
                success=False,
                message="No URL provided",
                error="No URL provided",
                url=url,
            )

        filename = self.generate_filename(paper_id)
        filepath = self.output_dir / filename

        if filepath.exists() and not overwrite:
            if self.is_valid_pdf(filepath):
                return DownloadResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Already exists: {filename}",
                    filepath=filepath,
                    file_size_bytes=filepath.stat().st_size,
                    url=url,
                )
            filepath.unlink(missing_ok=True)

        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=DOWNLOAD_TIMEOUT_SEC,
                    stream=True,
                )
                if response.status_code in {403, 404}:
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message=f"{response.status_code} while downloading: {url}",
                        error=str(response.status_code),
                        url=url,
                    )
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "").lower()
                if "pdf" not in content_type and "application/octet-stream" not in content_type:
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message=f"Not a PDF (Content-Type: {content_type})",
                        error=f"Invalid content type: {content_type}",
                        url=url,
                    )

                with filepath.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES):
                        if chunk:
                            handle.write(chunk)

                if not self.is_valid_pdf(filepath):
                    filepath.unlink(missing_ok=True)
                    if attempt < self.max_retries - 1:
                        time.sleep(2)
                        continue
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message="Downloaded file is not a valid PDF",
                        error="Invalid PDF file",
                        url=url,
                    )

                return DownloadResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Downloaded: {filename}",
                    filepath=filepath,
                    file_size_bytes=filepath.stat().st_size,
                    url=url,
                )
            except requests.exceptions.RequestException as exc:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    continue
                return DownloadResult(
                    paper_id=paper_id,
                    success=False,
                    message=f"Request error: {exc}",
                    error=str(exc),
                    url=url,
                )

        return DownloadResult(
            paper_id=paper_id,
            success=False,
            message="Failed after retries",
            error="Max retries exceeded",
            url=url,
        )

    def download_papers(
        self,
        papers: list[dict],
        paper_id_key: str = "paperId",
        url_key: str = "url",
        title_key: str = "title",
        overwrite: bool = False,
    ) -> list[DownloadResult]:
        results: list[DownloadResult] = []
        for paper in papers:
            result = self.download_paper(
                paper_id=paper.get(paper_id_key, "unknown"),
                url=paper.get(url_key),
                title=paper.get(title_key),
                overwrite=overwrite,
            )
            results.append(result)
            if self.delay > 0:
                time.sleep(self.delay)
        logger.info("Downloaded %s/%s PDFs", sum(result.success for result in results), len(results))
        return results
