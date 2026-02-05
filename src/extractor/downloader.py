import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import hashlib
import re


class PDFDownloader:
    """
    Downloads PDFs from URLs and saves them with paper ID as filename.
    
    Usage:
        downloader = PDFDownloader()  # Uses default: src/extractor/pdfs/
        downloader.download_paper("paper_id_123", "https://example.com/paper.pdf")
    """
    
    def __init__(self, output_dir: str = None, max_retries: int = 3):
        """
        Initialize the PDF downloader.
        
        Args:
            output_dir: Directory to save downloaded PDFs (default: src/extractor/pdfs/)
            max_retries: Maximum number of download retry attempts
        """
        if output_dir is None:
            # Default to src/extractor/pdfs/ relative to this file
            output_dir = Path(__file__).parent / "pdfs"
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        
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


# Example usage
if __name__ == "__main__":
    import json
    from pathlib import Path
    
    # Initialize downloader (uses default: src/extractor/pdfs/)
    downloader = PDFDownloader()
    
    # Option 1: Download a single paper
    success, message = downloader.download_paper(
        paper_id="649def34f8be52c8b66281af98ae884c09aef38b",
        url="https://arxiv.org/pdf/2104.14294.pdf"
    )
    print(f"Single download: {message}")
    
    # Option 2: Download from JSON file
    json_path = Path(__file__).parent.parent.parent / 'outputs' / 'retrieved_results.json'
    
    if json_path.exists():
        print(f"\nLoading papers from {json_path}...")
        with open(json_path, 'r', encoding='utf-8') as f:
            papers = json.load(f)
        
        # Filter papers with PDFs
        papers_with_pdfs = [
            p for p in papers 
            if p.get('openAccessPdf', {}).get('url')
        ]
        
        print(f"Found {len(papers_with_pdfs)} papers with PDF URLs")
        
        # Download all papers
        results = downloader.download_papers_from_list(
            papers_with_pdfs,
            delay=0.5,  # Be respectful to servers
            overwrite=False
        )
        
        # Print statistics
        downloader.print_statistics()
        
        # Save results
        results_path = Path(__file__).parent.parent.parent / 'outputs' / 'download_results.json'
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {results_path}")