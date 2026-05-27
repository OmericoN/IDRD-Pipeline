"""PDF to TEI XML conversion through an already-running GROBID service."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

from idrd.config import (
    CONVERSION_DELAY_SEC,
    GROBID_ALIVE_CHECK_TIMEOUT_SEC,
    GROBID_BASE_URL,
    GROBID_CONVERSION_TIMEOUT_SEC,
    GROBID_STARTUP_TIMEOUT_SEC,
    PDF_DIR,
    XML_DIR,
)
from idrd.models.results import ConversionResult

logger = logging.getLogger(__name__)


class GrobidConverter:
    """Convert PDFs using a configured GROBID HTTP endpoint."""

    def __init__(
        self,
        pdf_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        grobid_base_url: str = GROBID_BASE_URL,
        delay: float = CONVERSION_DELAY_SEC,
    ):
        self.pdf_dir = Path(pdf_dir) if pdf_dir else PDF_DIR
        self.output_dir = Path(output_dir) if output_dir else XML_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.grobid_base_url = grobid_base_url.rstrip("/")
        self.delay = delay

    def ensure_available(self, timeout: int = GROBID_STARTUP_TIMEOUT_SEC) -> None:
        start = time.time()
        while time.time() - start < timeout:
            try:
                response = requests.get(
                    f"{self.grobid_base_url}/api/isalive",
                    timeout=GROBID_ALIVE_CHECK_TIMEOUT_SEC,
                )
                if response.status_code == 200:
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        raise RuntimeError(f"GROBID is not reachable at {self.grobid_base_url}")

    def convert_pdf(
        self,
        pdf_path: Path,
        paper_id: str | None = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
    ) -> ConversionResult:
        pdf_path = Path(pdf_path)
        paper_id = paper_id or pdf_path.stem
        output_path = self.output_dir / f"{paper_id}.tei.xml"

        if not pdf_path.exists():
            return ConversionResult(
                paper_id=paper_id,
                success=False,
                message=f"PDF not found: {pdf_path}",
                pdf_path=pdf_path,
                error="PDF file not found",
            )

        if output_path.exists() and not overwrite:
            if delete_pdf:
                pdf_path.unlink(missing_ok=True)
            return ConversionResult(
                paper_id=paper_id,
                success=True,
                message=f"Already converted: {paper_id}.tei.xml",
                xml_path=output_path,
                pdf_path=pdf_path,
                xml_size_bytes=output_path.stat().st_size,
            )

        try:
            with pdf_path.open("rb") as handle:
                response = requests.post(
                    f"{self.grobid_base_url}/api/processFulltextDocument",
                    files={"input": handle},
                    timeout=GROBID_CONVERSION_TIMEOUT_SEC,
                )
            if response.status_code != 200:
                return ConversionResult(
                    paper_id=paper_id,
                    success=False,
                    message=f"GROBID error: {response.status_code}",
                    pdf_path=pdf_path,
                    error=f"GROBID HTTP {response.status_code}",
                )

            output_path.write_text(response.text, encoding="utf-8")
            if delete_pdf:
                pdf_path.unlink(missing_ok=True)
            return ConversionResult(
                paper_id=paper_id,
                success=True,
                message=f"Converted: {paper_id}.tei.xml",
                xml_path=output_path,
                pdf_path=pdf_path,
                xml_size_bytes=output_path.stat().st_size,
            )
        except requests.exceptions.RequestException as exc:
            return ConversionResult(
                paper_id=paper_id,
                success=False,
                message=f"Request error: {exc}",
                pdf_path=pdf_path,
                error=str(exc),
            )

    def convert_papers(
        self,
        papers: list[dict],
        paper_id_key: str = "paperId",
        pdf_path_key: str = "pdf_path",
        overwrite: bool = False,
        delete_pdf: bool = False,
    ) -> list[ConversionResult]:
        if not papers:
            return []
        self.ensure_available()

        results: list[ConversionResult] = []
        for paper in papers:
            pdf_path = paper.get(pdf_path_key)
            if not pdf_path:
                results.append(
                    ConversionResult(
                        paper_id=paper.get(paper_id_key, "unknown"),
                        success=False,
                        message="Missing PDF path",
                        error="Missing PDF path",
                    )
                )
                continue
            results.append(
                self.convert_pdf(
                    pdf_path=Path(pdf_path),
                    paper_id=paper.get(paper_id_key),
                    overwrite=overwrite,
                    delete_pdf=delete_pdf,
                )
            )
            if self.delay > 0:
                time.sleep(self.delay)

        logger.info("Converted %s/%s PDFs", sum(result.success for result in results), len(results))
        return results
