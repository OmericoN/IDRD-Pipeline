import docker
from docker.errors import NotFound, APIError, ImageNotFound
import requests
import time
import warnings
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import json
import sys

sys.path.append(str(Path(__file__).parent.parent))

from db.db import IDRDDatabase
from config import (
    POSTGRES_DSN,
    PDF_DIR,
    XML_DIR,
    GROBID_STARTUP_TIMEOUT_SEC,
    GROBID_ALIVE_CHECK_TIMEOUT_SEC,
    GROBID_CONVERSION_TIMEOUT_SEC,
    GROBID_STARTUP_RETRY_TIMEOUT_SEC,
    CONVERSION_DELAY_SEC,
)
from models.results import ConversionResult, PipelineStats
from utils.db_utils import update_xml_status

logger = logging.getLogger(__name__)


class GrobidConverter:
    """
    Converts PDFs to TEI XML via GROBID with flexible storage options.

    Decoupled from database - returns ConversionResult objects that can be persisted
    to database, DataFrame, JSON, or any other storage backend.

    (Requirements: Docker running in the background)

    ...
    Attributes
    ----------
    pdf_dir : str  (default = PDF_DIR, as per the config.py)
        the directory of the downloaded PDFs
    output_dir : str (default = XML_DIR, as per the config.py)
        the target directory of the converted TEI-XML
    """

    def __init__(
        self,
        pdf_dir: str = None,
        output_dir: str = None,
        grobid_port: int = 8070,
        container_name: str = "grobid-server",
        db: "IDRDDatabase" = None,
    ):
        """
        Args:
            pdf_dir: Directory containing PDF files
            output_dir: Directory for XML output
            grobid_port: Port for GROBID service
            container_name: Docker container name
            db: (Optional) Database instance for backward compatibility
        """
        self.pdf_dir = Path(pdf_dir) if pdf_dir else PDF_DIR
        self.output_dir = Path(output_dir) if output_dir else XML_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.grobid_port = grobid_port
        self.container_name = container_name
        self.grobid_url = f"http://localhost:{grobid_port}"

        try:
            self.docker_client = docker.from_env()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker not available: {e}")

        self.container = None
        self.stats = {"successful": 0, "failed": 0, "skipped": 0}

        # DB is now optional - only needed for legacy methods
        self._owns_db = db is None  # Own DB if none was provided
        self.db = db

        logger.info("GROBID Converter initialized")
        logger.info("PDF directory: %s", self.pdf_dir.absolute())
        logger.info("Output directory: %s", self.output_dir.absolute())
        logger.info("GROBID URL: %s", self.grobid_url)

    # ------------------------------------------------------------------
    # Docker helpers  (unchanged logic)
    # ------------------------------------------------------------------

    def _pull_grobid_image(self):
        image_name = "lfoppiano/grobid:0.8.0"
        try:
            self.docker_client.images.get(image_name)
            logger.info("GROBID image available: %s", image_name)
        except ImageNotFound:
            logger.info("Pulling GROBID image: %s...", image_name)
            self.docker_client.images.pull(image_name)
            logger.info("GROBID image pulled")

    def start_grobid(self, wait_time: int = 30):
        self._pull_grobid_image()
        try:
            self.container = self.docker_client.containers.get(self.container_name)
            if self.container.status == "running":
                if self._wait_for_grobid(timeout=5):
                    return
                self.container.restart()
            else:
                self.container.start()
        except NotFound:
            self.container = self.docker_client.containers.run(
                "lfoppiano/grobid:0.8.0",
                name=self.container_name,
                ports={"8070/tcp": self.grobid_port},
                detach=True,
                remove=False,
            )

        if not self._wait_for_grobid(timeout=wait_time):
            raise RuntimeError("GROBID did not start within timeout period")
        logger.info("GROBID is ready")

    def _wait_for_grobid(self, timeout: int = GROBID_STARTUP_TIMEOUT_SEC) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            try:
                r = requests.get(
                    f"{self.grobid_url}/api/isalive",
                    timeout=GROBID_ALIVE_CHECK_TIMEOUT_SEC,
                )
                if r.status_code == 200:
                    return True
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        return False

    def _is_grobid_available(self) -> bool:
        """Check if GROBID service is accessible (quick check)."""
        try:
            r = requests.get(f"{self.grobid_url}/api/isalive", timeout=2)
            return r.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def stop_grobid(self):
        if self.container:
            try:
                self.container.stop()
                logger.info("GROBID container stopped")
            except Exception as e:
                logger.warning("Could not stop GROBID: %s", e)

    # ------------------------------------------------------------------
    # Core conversion (returns ConversionResult)
    # ------------------------------------------------------------------

    def convert_pdf(
        self,
        pdf_path: Path,
        paper_id: str = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
    ) -> ConversionResult:
        """
        Convert a single PDF to TEI XML and return structured result.

        Returns:
            ConversionResult with success status, XML path, error info, etc.
        """
        if not pdf_path.exists():
            self.stats["failed"] += 1
            return ConversionResult(
                paper_id=paper_id or pdf_path.stem,
                success=False,
                message=f"PDF not found: {pdf_path}",
                pdf_path=pdf_path,
                error="PDF file not found",
            )

        paper_id = paper_id or pdf_path.stem
        output_path = self.output_dir / f"{paper_id}.tei.xml"

        if output_path.exists() and not overwrite:
            self.stats["skipped"] += 1
            if delete_pdf:
                try:
                    pdf_path.unlink()
                except Exception:
                    pass
            xml_size = output_path.stat().st_size
            return ConversionResult(
                paper_id=paper_id,
                success=True,
                message=f"Already converted: {paper_id}.tei.xml",
                xml_path=output_path,
                pdf_path=pdf_path,
                xml_size_bytes=xml_size,
            )

        try:
            with open(pdf_path, "rb") as f:
                response = requests.post(
                    f"{self.grobid_url}/api/processFulltextDocument",
                    files={"input": f},
                    timeout=GROBID_CONVERSION_TIMEOUT_SEC,
                )

            if response.status_code == 200:
                output_path.write_text(response.text, encoding="utf-8")
                self.stats["successful"] += 1
                xml_size = output_path.stat().st_size

                if delete_pdf:
                    try:
                        pdf_path.unlink()
                    except Exception:
                        pass

                size_kb = xml_size / 1024
                return ConversionResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Converted: {paper_id}.tei.xml ({size_kb:.1f} KB)",
                    xml_path=output_path,
                    pdf_path=pdf_path,
                    xml_size_bytes=xml_size,
                )

            elif response.status_code == 503:
                self.stats["failed"] += 1
                return ConversionResult(
                    paper_id=paper_id,
                    success=False,
                    message="GROBID service unavailable (503)",
                    pdf_path=pdf_path,
                    error="GROBID service unavailable",
                )
            else:
                self.stats["failed"] += 1
                return ConversionResult(
                    paper_id=paper_id,
                    success=False,
                    message=f"GROBID error: {response.status_code}",
                    pdf_path=pdf_path,
                    error=f"GROBID HTTP {response.status_code}",
                )

        except requests.exceptions.Timeout:
            self.stats["failed"] += 1
            return ConversionResult(
                paper_id=paper_id,
                success=False,
                message="Conversion timeout (>5 min)",
                pdf_path=pdf_path,
                error="Request timeout",
            )
        except Exception as e:
            self.stats["failed"] += 1
            return ConversionResult(
                paper_id=paper_id,
                success=False,
                message=f"Error: {str(e)}",
                pdf_path=pdf_path,
                error=str(e),
            )

    # ------------------------------------------------------------------
    # NEW API: Results-based batch conversion (database-agnostic)
    # ------------------------------------------------------------------

    def convert_papers(
        self,
        papers: List[Dict],
        paper_id_key: str = "paperId",
        pdf_path_key: str = "pdf_path",
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = CONVERSION_DELAY_SEC,
    ) -> List[ConversionResult]:
        """
        Convert PDFs to TEI XML from a list of paper dictionaries (database-agnostic).

        This method works with any data source - DataFrame, JSON, database query result, etc.
        Results can be persisted to database, DataFrame, or any storage backend.

        Args:
            papers: List of dictionaries containing paper metadata with PDF paths
            paper_id_key: Key for paper ID in dictionary (default: 'paperId')
            pdf_path_key: Key for PDF file path (default: 'pdf_path')
            overwrite: Whether to re-convert existing XMLs (default: False)
            delete_pdf: Whether to delete PDF after successful conversion (default: False)
            delay: Seconds to wait between conversions (default: 0.1)

        Returns:
            List of ConversionResult objects with success/failure info

        Example:
            >>> converter = GrobidConverter()
            >>> converter.start_grobid()
            >>> papers = [{'paperId': '123', 'pdf_path': '/path/to/file.pdf'}]
            >>> results = converter.convert_papers(papers)
            >>> successful = [r for r in results if r.success]
        """
        if not papers:
            logger.info("No papers to convert")
            return []

        logger.info("Converting %s PDFs...", len(papers))
        results = []

        with tqdm(total=len(papers), desc="Converting PDFs", unit="file") as pbar:
            for paper in papers:
                paper_id = paper.get(paper_id_key)
                pdf_path_str = paper.get(pdf_path_key)

                if not paper_id or not pdf_path_str:
                    result = ConversionResult(
                        paper_id=paper_id or "unknown",
                        success=False,
                        message="Missing paper ID or PDF path",
                        error="Missing required fields",
                    )
                    results.append(result)
                    self.stats["failed"] += 1
                    pbar.update(1)
                    continue

                pdf_path = Path(pdf_path_str)
                result = self.convert_pdf(pdf_path, paper_id, overwrite, delete_pdf)
                results.append(result)
                pbar.set_postfix_str(result.message[:50])
                pbar.update(1)

                if delay > 0:
                    time.sleep(delay)

        return results

    # ------------------------------------------------------------------
    # LEGACY API: Database-coupled methods (deprecated)
    # ------------------------------------------------------------------

    def convert_from_database(
        self,
        limit: int = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = 0.1,
    ) -> Dict:
        """
        DEPRECATED: Use convert_papers() + persist_conversion_results() instead.

        Legacy method that couples conversion with database updates.
        Kept for backward compatibility with existing pipeline code.
        """
        warnings.warn(
            "convert_from_database() is deprecated. Use convert_papers() with "
            "persist_conversion_results() for better separation of concerns.",
            DeprecationWarning,
            stacklevel=2,
        )

        if self.db is None:
            raise ValueError(
                "Database not provided to constructor - cannot use convert_from_database()"
            )

        papers = self.db.get_papers_needing_conversion(limit=limit)

        if not papers:
            logger.info("No PDFs need conversion")
            return {"results": [], "stats": self.stats}

        logger.info("Converting %s PDFs...", len(papers))
        results = []

        with tqdm(total=len(papers), desc="Converting PDFs", unit="file") as pbar:
            for row in papers:
                paper_id = row["paperId"]
                pdf_path = Path(row["pdf_path"])

                if not pdf_path.exists():
                    msg = f"PDF not found: {pdf_path}"
                    update_xml_status(self.db, paper_id, False, error=msg)
                    results.append(
                        {"paper_id": paper_id, "success": False, "message": msg}
                    )
                    pbar.update(1)
                    continue

                result = self.convert_pdf(pdf_path, paper_id, overwrite, delete_pdf)

                # Update database (legacy behavior)
                xml_path = str(result.xml_path) if result.xml_path else None
                update_xml_status(
                    self.db,
                    paper_id,
                    result.success,
                    xml_path=xml_path,
                    error=result.error,
                )

                results.append(
                    {
                        "paper_id": paper_id,
                        "success": result.success,
                        "message": result.message,
                    }
                )
                pbar.set_postfix_str(result.message[:50])
                pbar.update(1)
                if delay > 0:
                    time.sleep(delay)

        return {"results": results, "stats": self.stats.copy()}

    # ------------------------------------------------------------------
    # Stats / lifecycle
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict:
        total = self.stats["successful"] + self.stats["failed"]
        return {
            **self.stats,
            "success_rate": (self.stats["successful"] / total * 100)
            if total > 0
            else 0,
        }

    def print_statistics(self):
        stats = self.get_statistics()
        logger.info("%s\nCONVERSION STATISTICS\n%s", "=" * 60, "=" * 60)
        logger.info("Successful:   %s", stats["successful"])
        logger.info("Failed:       %s", stats["failed"])
        logger.info("Skipped:      %s", stats["skipped"])
        logger.info("Success rate: %.1f%%", stats["success_rate"])
        logger.info("%s", "=" * 60)

    def close_db(self):
        """Close database connection if we own it."""
        if self._owns_db and self.db is not None:
            self.db.close()

    def __enter__(self):
        self.start_grobid()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_grobid()
        self.close_db()
