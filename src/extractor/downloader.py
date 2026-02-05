import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import hashlib
import re
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from db.db import PublicationDatabase


class PDFDownloader:
    """
    Downloads PDFs from URLs and saves them with paper ID as filename.
    Updates database with download status.
    
    Usage:
        downloader = PDFDownloader()  # Uses default: outputs/pdf/
        downloader.download_from_database()
    """
    
    def __init__(self, output_dir: str = None, max_retries: int = 3, db_path: str = None):
        """
        Initialize the PDF downloader.
        
        Args:
            output_dir: Directory to save downloaded PDFs (default: outputs/pdf/)
            max_retries: Maximum number of download retry attempts
            db_path: Path to database (default: src/db/publications.db)
        """
        if output_dir is None:
            output_dir = Path(__file__).parent.parent.parent / "outputs" / "pdf"
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        
        # Initialize database connection
        self.db = PublicationDatabase(db_path)
        
        # Headers to mimic a browser request
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Track download statistics
        self.stats = {
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': 0
        }
        
        print(f"✓ PDF Downloader initialized")
        print(f"  Output directory: {self.output_dir.absolute()}")
    
    def generate_filename(self, paper_id: str) -> str:
        """
        Generate a filename for the PDF using only paper ID.
        
        Args:
            paper_id: Unique paper identifier
            
        Returns:
            Filename with .pdf extension (e.g., "abc123.pdf")
        """
        # Sanitize paper_id to remove any invalid characters
        sanitized_id = re.sub(r'[<>:"/\\|?*]', '', paper_id)
        return f"{sanitized_id}.pdf"
    
    def is_valid_pdf(self, filepath: Path) -> bool:
        """
        Check if file is a valid PDF by checking magic bytes.
        
        Args:
            filepath: Path to the file
            
        Returns:
            True if file appears to be a valid PDF
        """
        try:
            with open(filepath, 'rb') as f:
                header = f.read(4)
                return header == b'%PDF'
        except Exception:
            return False
    
    def download_paper(
        self,
        paper_id: str,
        url: str,
        title: Optional[str] = None,
        overwrite: bool = False
    ) -> Tuple[bool, str]:
        """
        Download a single paper PDF.
        
        Args:
            paper_id: Unique identifier for the paper
            url: URL to download the PDF from
            title: Optional paper title (not used for filename, kept for compatibility)
            overwrite: If True, re-download even if file exists
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        if not url:
            self.stats['skipped'] += 1
            return False, "No URL provided"
        
        # Generate filename (paper_id only)
        filename = self.generate_filename(paper_id)
        filepath = self.output_dir / filename
        
        # Check if already downloaded
        if filepath.exists() and not overwrite:
            if self.is_valid_pdf(filepath):
                self.stats['skipped'] += 1
                return True, f"Already exists: {filename}"
            else:
                # Invalid PDF, delete and re-download
                filepath.unlink()
        
        # Try downloading with retries
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=60,
                    stream=True
                )
                
                # Check response status
                if response.status_code == 404:
                    self.stats['failed'] += 1
                    return False, f"404 Not Found: {url}"
                
                if response.status_code == 403:
                    self.stats['failed'] += 1
                    return False, f"403 Forbidden: {url}"
                
                response.raise_for_status()
                
                # Check if content is actually a PDF
                content_type = response.headers.get('Content-Type', '').lower()
                if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
                    self.stats['failed'] += 1
                    return False, f"Not a PDF (Content-Type: {content_type})"
                
                # Download and save
                total_size = int(response.headers.get('content-length', 0))
                
                with open(filepath, 'wb') as f:
                    if total_size == 0:
                        # No content length header, download all at once
                        f.write(response.content)
                    else:
                        # Download with progress
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                
                # Verify downloaded file is a valid PDF
                if not self.is_valid_pdf(filepath):
                    filepath.unlink()  # Delete invalid file
                    if attempt < self.max_retries - 1:
                        time.sleep(2)
                        continue
                    self.stats['failed'] += 1
                    return False, "Downloaded file is not a valid PDF"
                
                # Success
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
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    continue
                self.stats['failed'] += 1
                return False, f"Unexpected error: {str(e)}"
        
        self.stats['failed'] += 1
        return False, f"Failed after {self.max_retries} attempts"
    
    def download_papers_from_list(
        self,
        papers: List[Dict],
        paper_id_key: str = 'paperId',
        url_key: str = 'openAccessPdf',
        title_key: str = 'title',
        delay: float = 0.5,
        overwrite: bool = False
    ) -> Dict:
        """
        Download multiple papers from a list of paper dictionaries.
        
        Args:
            papers: List of paper dictionaries
            paper_id_key: Key for paper ID in dictionary
            url_key: Key for URL in dictionary (can be nested like 'openAccessPdf.url')
            title_key: Key for title in dictionary (kept for logging)
            delay: Delay between downloads in seconds
            overwrite: If True, re-download existing files
            
        Returns:
            Dictionary with download statistics and results
        """
        print(f"\nDownloading {len(papers)} papers...")
        
        results = []
        
        with tqdm(total=len(papers), desc="Downloading PDFs", unit="paper") as pbar:
            for paper in papers:
                paper_id = paper.get(paper_id_key)
                title = paper.get(title_key)
                
                # Handle nested URL key (e.g., 'openAccessPdf.url')
                url = paper
                for key in url_key.split('.'):
                    url = url.get(key, {}) if isinstance(url, dict) else None
                    if url is None:
                        break
                
                # If url is still a dict, try to get 'url' key
                if isinstance(url, dict):
                    url = url.get('url')
                
                if not paper_id or not url:
                    results.append({
                        'paper_id': paper_id,
                        'title': title,
                        'success': False,
                        'message': 'Missing paper ID or URL'
                    })
                    self.stats['skipped'] += 1
                    pbar.update(1)
                    continue
                
                success, message = self.download_paper(paper_id, url, title, overwrite)
                
                results.append({
                    'paper_id': paper_id,
                    'title': title,
                    'url': url,
                    'success': success,
                    'message': message
                })
                
                pbar.update(1)
                
                # Delay to be respectful to servers
                if delay > 0:
                    time.sleep(delay)
        
        return {
            'results': results,
            'stats': self.stats.copy()
        }
    
    def download_from_database(
        self,
        limit: int = None,
        overwrite: bool = False,
        delay: float = 0.5
    ) -> Dict:
        """
        Download PDFs for papers in database that haven't been downloaded yet.
        
        Args:
            limit: Maximum number of papers to download (None = all)
            overwrite: If True, re-download existing PDFs
            delay: Delay between downloads in seconds
            
        Returns:
            Dictionary with download statistics
        """
        # Query papers that need PDFs
        if overwrite:
            query = '''
                SELECT p.paperId, p.title, oa.url
                FROM publications p
                JOIN open_access oa ON p.paperId = oa.paperId
                WHERE oa.url IS NOT NULL
            '''
        else:
            query = '''
                SELECT p.paperId, p.title, oa.url
                FROM publications p
                JOIN open_access oa ON p.paperId = oa.paperId
                WHERE oa.url IS NOT NULL 
                AND (p.pdf_downloaded = 0 OR p.pdf_downloaded IS NULL)
            '''
        
        if limit:
            query += f' LIMIT {limit}'
        
        self.db.cursor.execute(query)
        papers_to_download = self.db.cursor.fetchall()
        
        if not papers_to_download:
            print("No papers need PDF download")
            return {'results': [], 'stats': self.stats}
        
        print(f"\nDownloading {len(papers_to_download)} PDFs from database...")
        
        results = []
        
        with tqdm(total=len(papers_to_download), desc="Downloading PDFs", unit="paper") as pbar:
            for row in papers_to_download:
                paper_id = row[0]
                title = row[1]
                url = row[2]
                
                success, message = self.download_paper(paper_id, url, title, overwrite)
                
                # Update database
                if success:
                    pdf_path = str(self.output_dir / f"{paper_id}.pdf")
                    self.db.cursor.execute('''
                        UPDATE publications 
                        SET pdf_downloaded = 1,
                            pdf_download_date = CURRENT_TIMESTAMP,
                            pdf_path = ?,
                            pdf_download_error = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE paperId = ?
                    ''', (pdf_path, paper_id))
                else:
                    self.db.cursor.execute('''
                        UPDATE publications 
                        SET pdf_downloaded = 0,
                            pdf_download_error = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE paperId = ?
                    ''', (message, paper_id))
                
                self.db.conn.commit()
                
                results.append({
                    'paper_id': paper_id,
                    'title': title,
                    'url': url,
                    'success': success,
                    'message': message
                })
                
                pbar.update(1)
                
                if delay > 0:
                    time.sleep(delay)
        
        return {
            'results': results,
            'stats': self.stats.copy()
        }
    
    def get_statistics(self) -> Dict:
        """Get download statistics."""
        total = self.stats['successful'] + self.stats['failed']
        return {
            **self.stats,
            'success_rate': (self.stats['successful'] / total * 100) if total > 0 else 0,
            'total_size_mb': self.stats['total_size'] / 1024 / 1024,
            'avg_size_mb': (self.stats['total_size'] / self.stats['successful'] / 1024 / 1024) 
                          if self.stats['successful'] > 0 else 0
        }
    
    def print_statistics(self):
        """Print download statistics."""
        stats = self.get_statistics()
        print(f"\n{'='*60}")
        print("DOWNLOAD STATISTICS")
        print(f"{'='*60}")
        print(f"Successful:    {stats['successful']}")
        print(f"Failed:        {stats['failed']}")
        print(f"Skipped:       {stats['skipped']}")
        print(f"Success rate:  {stats['success_rate']:.1f}%")
        print(f"Total size:    {stats['total_size_mb']:.2f} MB")
        print(f"Average size:  {stats['avg_size_mb']:.2f} MB")
        print(f"{'='*60}")
    
    def close(self):
        """Close database connection."""
        self.db.close()


# Example usage
if __name__ == "__main__":
    import json
    
    # Initialize downloader with database connection
    downloader = PDFDownloader()
    
    try:
        # First, check database status
        print("\n" + "="*60)
        print("DATABASE STATUS CHECK")
        print("="*60)
        
        # Check total papers in database
        downloader.db.cursor.execute('SELECT COUNT(*) FROM publications')
        total_papers = downloader.db.cursor.fetchone()[0]
        print(f"Total papers in database: {total_papers}")
        
        # Check papers with open access URLs
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications p
            JOIN open_access oa ON p.paperId = oa.paperId
            WHERE oa.url IS NOT NULL
        ''')
        papers_with_urls = downloader.db.cursor.fetchone()[0]
        print(f"Papers with PDF URLs: {papers_with_urls}")
        
        # Check papers already downloaded
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications 
            WHERE pdf_downloaded = 1
        ''')
        already_downloaded = downloader.db.cursor.fetchone()[0]
        print(f"Papers marked as downloaded in DB: {already_downloaded}")
        
        # Check existing PDF files
        existing_pdfs = list(downloader.output_dir.glob("*.pdf"))
        print(f"Existing PDF files on disk: {len(existing_pdfs)}")
        
        # Check papers needing download
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications p
            JOIN open_access oa ON p.paperId = oa.paperId
            WHERE oa.url IS NOT NULL 
            AND (p.pdf_downloaded = 0 OR p.pdf_downloaded IS NULL)
        ''')
        papers_needing_download = downloader.db.cursor.fetchone()[0]
        print(f"Papers needing download: {papers_needing_download}")
        
        print("="*60)
        
        # If database is empty, suggest loading from JSON
        if total_papers == 0:
            print("\nWARNING: DATABASE IS EMPTY!")
            print("Please run one of the following:")
            print("  1. python src/pubfetcher/fetching.py  (to fetch new papers)")
            print("  2. python src/db/db.py  (to load from JSON)")
            downloader.close()
            exit(0)
        
        # If we have PDFs but database doesn't know about them, sync
        if len(existing_pdfs) > 0 and already_downloaded < len(existing_pdfs):
            print(f"\nSyncing {len(existing_pdfs)} existing PDFs with database...")
            synced = 0
            skipped = 0
            
            for pdf_file in tqdm(existing_pdfs, desc="Syncing PDFs", unit="file"):
                paper_id = pdf_file.stem
                pdf_path = str(pdf_file)
                
                # Check if this paper exists in database
                downloader.db.cursor.execute(
                    'SELECT paperId, pdf_downloaded FROM publications WHERE paperId = ?',
                    (paper_id,)
                )
                result = downloader.db.cursor.fetchone()
                
                if result:
                    if result[1] != 1:  # Not already marked as downloaded
                        # Update database to mark as downloaded
                        downloader.db.cursor.execute('''
                            UPDATE publications 
                            SET pdf_downloaded = 1,
                                pdf_download_date = CURRENT_TIMESTAMP,
                                pdf_path = ?,
                                pdf_download_error = NULL,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE paperId = ?
                        ''', (pdf_path, paper_id))
                        synced += 1
                    else:
                        skipped += 1
                else:
                    print(f"\nWARNING: PDF found but not in database: {paper_id}.pdf")
            
            downloader.db.conn.commit()
            print(f"Successfully synced {synced} PDFs with database")
            if skipped > 0:
                print(f"Skipped {skipped} already synced PDFs")
            print()
        
        # Now download any remaining papers
        print("\n" + "="*60)
        print("STARTING PDF DOWNLOAD")
        print("="*60)
        
        results = downloader.download_from_database(
            limit=None,
            overwrite=False,
            delay=0.5
        )
        
        # Print statistics
        downloader.print_statistics()
        
        # Show updated database status
        print("\n" + "="*60)
        print("FINAL DATABASE STATUS")
        print("="*60)
        
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications 
            WHERE pdf_downloaded = 1
        ''')
        final_downloaded = downloader.db.cursor.fetchone()[0]
        print(f"Total papers with PDFs: {final_downloaded}/{papers_with_urls}")
        
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications 
            WHERE pdf_download_error IS NOT NULL
        ''')
        errors = downloader.db.cursor.fetchone()[0]
        print(f"Papers with download errors: {errors}")
        
        print("="*60)
        
        # Save results
        results_path = Path(__file__).parent.parent.parent / 'outputs' / 'metadata' / 'download_results.json'
        results_path.parent.mkdir(parents=True, exist_ok=True)
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {results_path}")
        
    finally:
        downloader.close()
        print("\nDatabase connection closed")