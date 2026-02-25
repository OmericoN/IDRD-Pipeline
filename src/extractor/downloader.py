import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import re
import sys

sys.path.append(str(Path(__file__).parent.parent))

from db.db import PublicationDatabase
from config import PDF_DIR
from utils.db_utils import (
    print_download_status,
    sync_existing_pdfs,
    update_pdf_status,
)


class PDFDownloader:
    """Downloads PDFs and updates database via db.py methods."""

    def __init__(self, output_dir: str = None, db: PublicationDatabase = None):
        """
        Args:
            output_dir: Where to save PDFs. Defaults to config.PDF_DIR.
            db: Shared PublicationDatabase instance. If None, creates its own.
        """
        self.output_dir = Path(output_dir) if output_dir else PDF_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = 3

        # Accept shared DB or create own — avoids duplicate connections in pipeline
        self._owns_db = db is None
        self.db = db if db is not None else PublicationDatabase()

        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            )
        }
        self.stats = {'successful': 0, 'failed': 0, 'skipped': 0, 'total_size': 0}
        print(f"✓ PDF Downloader initialized")
        print(f"  Output directory: {self.output_dir.absolute()}")

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
    # Core download
    # ------------------------------------------------------------------

    def download_paper(
        self,
        paper_id: str,
        url: str,
        title: Optional[str] = None,
        overwrite: bool = False,
    ) -> Tuple[bool, str]:
        if not url:
            self.stats['skipped'] += 1
            return False, "No URL provided"

        filename = self.generate_filename(paper_id)
        filepath = self.output_dir / filename

        if filepath.exists() and not overwrite:
            if self.is_valid_pdf(filepath):
                self.stats['skipped'] += 1
                return True, f"Already exists: {filename}"
            filepath.unlink()

        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=60, stream=True)

                if response.status_code == 404:
                    self.stats['failed'] += 1
                    return False, f"404 Not Found: {url}"
                if response.status_code == 403:
                    self.stats['failed'] += 1
                    return False, f"403 Forbidden: {url}"

                response.raise_for_status()

                content_type = response.headers.get('Content-Type', '').lower()
                if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
                    self.stats['failed'] += 1
                    return False, f"Not a PDF (Content-Type: {content_type})"

                total_size = int(response.headers.get('content-length', 0))
                with open(filepath, 'wb') as f:
                    if total_size == 0:
                        f.write(response.content)
                    else:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                if not self.is_valid_pdf(filepath):
                    filepath.unlink()
                    if attempt < self.max_retries - 1:
                        time.sleep(2)
                        continue
                    self.stats['failed'] += 1
                    return False, "Downloaded file is not a valid PDF"

                file_size = filepath.stat().st_size
                self.stats['successful'] += 1
                self.stats['total_size'] += file_size
                return True, f"Downloaded: {filename} ({file_size / 1024 / 1024:.2f} MB)"

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    continue
                self.stats['failed'] += 1
                return False, f"Timeout after {self.max_retries} attempts"

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    continue
                self.stats['failed'] += 1
                return False, f"Request error: {str(e)}"

        self.stats['failed'] += 1
        return False, f"Failed after {self.max_retries} attempts"

    # ------------------------------------------------------------------
    # Batch helpers
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
        print(f"\nDownloading {len(papers)} papers...")
        results = []

        with tqdm(total=len(papers), desc="Downloading PDFs", unit="paper") as pbar:
            for paper in papers:
                paper_id = paper.get(paper_id_key)
                title    = paper.get(title_key)

                url = paper
                for key in url_key.split('.'):
                    url = url.get(key, {}) if isinstance(url, dict) else None
                    if url is None:
                        break
                if isinstance(url, dict):
                    url = url.get('url')

                if not paper_id or not url:
                    results.append({'paper_id': paper_id, 'success': False,
                                    'message': 'Missing paper ID or URL'})
                    self.stats['skipped'] += 1
                    pbar.update(1)
                    continue

                success, message = self.download_paper(paper_id, url, title, overwrite)
                results.append({'paper_id': paper_id, 'title': title,
                                 'url': url, 'success': success, 'message': message})
                pbar.update(1)
                if delay > 0:
                    time.sleep(delay)

        return {'results': results, 'stats': self.stats.copy()}

    def download_from_database(
        self,
        limit: int = None,
        overwrite: bool = False,
        delay: float = 0.5,
    ) -> Dict:
        papers_to_download = self.db.get_papers_needing_download(limit=limit)

        if not papers_to_download:
            print("No papers need PDF download")
            return {'results': [], 'stats': self.stats}

        print(f"\nDownloading {len(papers_to_download)} PDFs from database...")
        results = []

        with tqdm(total=len(papers_to_download), desc="Downloading PDFs", unit="paper") as pbar:
            for row in papers_to_download:
                paper_id = row['paperId']
                url      = row['url']

                success, message = self.download_paper(paper_id, url, overwrite=overwrite)

                pdf_path = str(self.output_dir / f"{paper_id}.pdf") if success else None
                update_pdf_status(self.db, paper_id, success, pdf_path=pdf_path, error=message)

                results.append({'paper_id': paper_id, 'url': url,
                                 'success': success, 'message': message})
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
        print(f"\n{'='*60}\nDOWNLOAD STATISTICS\n{'='*60}")
        print(f"Successful:   {stats['successful']}")
        print(f"Failed:       {stats['failed']}")
        print(f"Skipped:      {stats['skipped']}")
        print(f"Success rate: {stats['success_rate']:.1f}%")
        print(f"Total size:   {stats['total_size_mb']:.2f} MB")
        print(f"Average size: {stats['avg_size_mb']:.2f} MB")
        print(f"{'='*60}")

    def close(self):
        """Only close DB if we own it (not injected from outside)."""
        if self._owns_db:
            self.db.close()