import requests
import time
import warnings
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import re
import sys

sys.path.append(str(Path(__file__).parent.parent))

from db.db import IDRDDatabase
from config import (
    PDF_DIR,
    DOWNLOAD_TIMEOUT_SEC,
    DOWNLOAD_CHUNK_SIZE_BYTES,
    DOWNLOAD_DELAY_SEC,
)
from models.results import DownloadResult, PipelineStats
from utils.db_utils import (
    print_download_status,
    sync_existing_pdfs,
    update_pdf_status,
)

logger = logging.getLogger(__name__)


class PDFDownloader:
    """
    Downloads PDFs with flexible storage options.
    
    Decoupled from database - returns DownloadResult objects that can be persisted
    to database, DataFrame, JSON, or any other storage backend.
    """

    def __init__(self, output_dir: str = None, db: IDRDDatabase = None):
        """
        Args:
            output_dir: Where to save PDFs. Defaults to config.PDF_DIR.
            db: (Optional) Shared IDRDDatabase instance for backward compatibility.
                New code should use download_papers() and persist results separately.
        """
        self.output_dir = Path(output_dir) if output_dir else PDF_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = 3

        # DB is now optional - only needed for legacy download_from_database() method
        self._owns_db = db is None if db is not None else False
        self.db = db

        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            )
        }
        self.stats = {'successful': 0, 'failed': 0, 'skipped': 0, 'total_size': 0}
        logger.info("PDF Downloader initialized")
        logger.info("Output directory: %s", self.output_dir.absolute())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def generate_filename(self, paper_id: str) -> str:
        return f"{re.sub(r'[<>:\"/\\|?*]', '', paper_id)}.pdf"

    def is_valid_pdf(self, filepath: Path) -> bool:
        try:
            with open(filepath, 'rb') as f:
                return f.read(4) == b'%PDF'
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Core download (returns DownloadResult)
    # ------------------------------------------------------------------

    def download_paper(
        self,
        paper_id: str,
        url: str,
        title: Optional[str] = None,
        overwrite: bool = False,
    ) -> DownloadResult:
        """
        Download a single PDF and return structured result.
        
        Returns:
            DownloadResult with success status, filepath, error info, etc.
        """
        if not url:
            self.stats['skipped'] += 1
            return DownloadResult(
                paper_id=paper_id,
                success=False,
                message="No URL provided",
                error="No URL provided",
                url=url
            )

        filename = self.generate_filename(paper_id)
        filepath = self.output_dir / filename

        if filepath.exists() and not overwrite:
            if self.is_valid_pdf(filepath):
                self.stats['skipped'] += 1
                file_size = filepath.stat().st_size
                self.stats['total_size'] += file_size
                return DownloadResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Already exists: {filename}",
                    filepath=filepath,
                    file_size_bytes=file_size,
                    url=url
                )
            filepath.unlink()

        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=DOWNLOAD_TIMEOUT_SEC, stream=True)

                if response.status_code == 404:
                    self.stats['failed'] += 1
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message=f"404 Not Found: {url}",
                        error="404 Not Found",
                        url=url
                    )
                if response.status_code == 403:
                    self.stats['failed'] += 1
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message=f"403 Forbidden: {url}",
                        error="403 Forbidden",
                        url=url
                    )

                response.raise_for_status()

                content_type = response.headers.get('Content-Type', '').lower()
                if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
                    self.stats['failed'] += 1
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message=f"Not a PDF (Content-Type: {content_type})",
                        error=f"Invalid content type: {content_type}",
                        url=url
                    )

                total_size = int(response.headers.get('content-length', 0))
                with open(filepath, 'wb') as f:
                    if total_size == 0:
                        f.write(response.content)
                    else:
                        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES):
                            if chunk:
                                f.write(chunk)

                if not self.is_valid_pdf(filepath):
                    filepath.unlink()
                    if attempt < self.max_retries - 1:
                        time.sleep(2)
                        continue
                    self.stats['failed'] += 1
                    return DownloadResult(
                        paper_id=paper_id,
                        success=False,
                        message="Downloaded file is not a valid PDF",
                        error="Invalid PDF file",
                        url=url
                    )

                file_size = filepath.stat().st_size
                self.stats['successful'] += 1
                self.stats['total_size'] += file_size
                return DownloadResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Downloaded: {filename} ({file_size / 1024 / 1024:.2f} MB)",
                    filepath=filepath,
                    file_size_bytes=file_size,
                    url=url
                )

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    continue
                self.stats['failed'] += 1
                return DownloadResult(
                    paper_id=paper_id,
                    success=False,
                    message=f"Timeout after {self.max_retries} attempts",
                    error="Request timeout",
                    url=url
                )

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    continue
                self.stats['failed'] += 1
                return DownloadResult(
                    paper_id=paper_id,
                    success=False,
                    message=f"Request error: {str(e)}",
                    error=str(e),
                    url=url
                )

        self.stats['failed'] += 1
        return DownloadResult(
            paper_id=paper_id,
            success=False,
            message=f"Failed after {self.max_retries} attempts",
            error="Max retries exceeded",
            url=url
        )

    # ------------------------------------------------------------------
    # NEW API: Results-based batch download (database-agnostic)
    # ------------------------------------------------------------------

    def download_papers(
        self,
        papers: List[Dict],
        paper_id_key: str = 'paperId',
        url_key: str = 'url',
        title_key: str = 'title',
        delay: float = 0.5,
        overwrite: bool = False,
    ) -> List[DownloadResult]:
        """
        Download PDFs from a list of paper dictionaries (database-agnostic).
        
        This method works with any data source - DataFrame, JSON, API response, etc.
        Results can be persisted to database, DataFrame, or any storage backend.
        
        Args:
            papers: List of dictionaries containing paper metadata
            paper_id_key: Key for paper ID in dictionary (default: 'paperId')
            url_key: Key path for PDF URL (default: 'url', supports nested like 'openAccessPdf.url')
            title_key: Key for paper title (default: 'title')
            delay: Seconds to wait between downloads (default: 0.5)
            overwrite: Whether to re-download existing PDFs (default: False)
            
        Returns:
            List of DownloadResult objects with success/failure info
            
        Example:
            >>> downloader = PDFDownloader()
            >>> papers = [{'paperId': '123', 'url': 'http://...', 'title': 'Paper 1'}]
            >>> results = downloader.download_papers(papers)
            >>> successful = [r for r in results if r.success]
        """
        logger.info("Downloading %s papers...", len(papers))
        results = []

        with tqdm(total=len(papers), desc="Downloading PDFs", unit="paper") as pbar:
            for paper in papers:
                paper_id = paper.get(paper_id_key)
                title = paper.get(title_key)

                # Handle nested URL keys (e.g., 'openAccessPdf.url')
                url = paper
                for key in url_key.split('.'):
                    url = url.get(key, {}) if isinstance(url, dict) else None
                    if url is None:
                        break
                if isinstance(url, dict):
                    url = url.get('url')

                if not paper_id or not url:
                    result = DownloadResult(
                        paper_id=paper_id or 'unknown',
                        success=False,
                        message='Missing paper ID or URL',
                        error='Missing required fields',
                        url=url
                    )
                    results.append(result)
                    self.stats['skipped'] += 1
                    pbar.update(1)
                    continue

                result = self.download_paper(paper_id, url, title, overwrite)
                results.append(result)
                pbar.set_postfix_str(result.message[:50])
                pbar.update(1)
                
                if delay > 0:
                    time.sleep(delay)

        return results

    # ------------------------------------------------------------------
    # LEGACY API: Database-coupled methods (deprecated)
    # ------------------------------------------------------------------

    def download_papers_from_list(
        self,
        papers: List[Dict],
        paper_id_key: str = 'paperId',
        url_key: str = 'openAccessPdf',
        title_key: str = 'title',
        delay: float = 0.5,
        overwrite: bool = False,
    ) -> Dict:
        """
        DEPRECATED: Use download_papers() instead for cleaner, database-agnostic API.
        
        Legacy method that returns dict format for backward compatibility.
        """
        warnings.warn(
            "download_papers_from_list() is deprecated. Use download_papers() "
            "which returns List[DownloadResult] for better type safety.",
            DeprecationWarning,
            stacklevel=2
        )
        
        results_list = self.download_papers(
            papers, paper_id_key, url_key, title_key, delay, overwrite
        )
        
        # Convert to legacy dict format
        results = [
            {
                'paper_id': r.paper_id,
                'title': papers[i].get(title_key) if i < len(papers) else None,
                'url': r.url,
                'success': r.success,
                'message': r.message
            }
            for i, r in enumerate(results_list)
        ]
        
        return {'results': results, 'stats': self.stats.copy()}

    def download_from_database(
        self,
        limit: int = None,
        overwrite: bool = False,
        delay: float = DOWNLOAD_DELAY_SEC,
    ) -> Dict:
        """
        DEPRECATED: Use download_papers() + persist_download_results() instead.
        
        Legacy method that couples downloading with database updates.
        Kept for backward compatibility with existing pipeline code.
        """
        warnings.warn(
            "download_from_database() is deprecated. Use download_papers() with "
            "persist_download_results() for better separation of concerns.",
            DeprecationWarning,
            stacklevel=2
        )
        
        if self.db is None:
            raise ValueError("Database not provided to constructor - cannot use download_from_database()")
        
        papers_to_download = self.db.get_papers_needing_download(limit=limit)

        if not papers_to_download:
            logger.info("No papers need PDF download")
            return {'results': [], 'stats': self.stats}

        logger.info("Downloading %s PDFs from database...", len(papers_to_download))
        results = []

        with tqdm(total=len(papers_to_download), desc="Downloading PDFs", unit="paper") as pbar:
            for row in papers_to_download:
                paper_id = row['paperId']
                url = row['url']

                result = self.download_paper(paper_id, url, overwrite=overwrite)

                # Update database (legacy behavior)
                pdf_path = str(result.filepath) if result.filepath else None
                update_pdf_status(self.db, paper_id, result.success, 
                                pdf_path=pdf_path, error=result.error)

                results.append({
                    'paper_id': paper_id,
                    'url': url,
                    'success': result.success,
                    'message': result.message
                })
                pbar.update(1)
                if delay > 0:
                    time.sleep(delay)

        return {'results': results, 'stats': self.stats.copy()}

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict:
        total = self.stats['successful'] + self.stats['failed']
        return {
            **self.stats,
            'success_rate':  (self.stats['successful'] / total * 100) if total > 0 else 0,
            'total_size_mb': self.stats['total_size'] / 1024 / 1024,
            'avg_size_mb':   (self.stats['total_size'] / self.stats['successful'] / 1024 / 1024)
                             if self.stats['successful'] > 0 else 0,
        }

    def print_statistics(self):
        stats = self.get_statistics()
        logger.info("%s\nDOWNLOAD STATISTICS\n%s", "=" * 60, "=" * 60)
        logger.info("Successful:   %s", stats['successful'])
        logger.info("Failed:       %s", stats['failed'])
        logger.info("Skipped:      %s", stats['skipped'])
        logger.info("Success rate: %.1f%%", stats['success_rate'])
        logger.info("Total size:   %.2f MB", stats['total_size_mb'])
        logger.info("Average size: %.2f MB", stats['avg_size_mb'])
        logger.info("%s", "=" * 60)

    def close(self):
        """Close database connection if we own it."""
        if self._owns_db and self.db is not None:
            self.db.close()
