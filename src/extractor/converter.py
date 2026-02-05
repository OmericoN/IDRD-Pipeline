import docker
from docker.errors import NotFound, APIError, ImageNotFound
import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import xml.etree.ElementTree as ET
import json
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from db.db import PublicationDatabase


class GrobidConverter:
    """
    Manages GROBID Docker container and converts PDFs to TEI XML.
    Updates database with conversion status.
    
    Usage:
        with GrobidConverter() as converter:
            converter.convert_from_database()
    """
    
    def __init__(
        self,
        pdf_dir: str = None,
        output_dir: str = None,
        grobid_port: int = 8070,
        container_name: str = "grobid-server",
        db_path: str = None
    ):
        """
        Initialize GROBID converter.
        
        Args:
            pdf_dir: Directory containing PDFs (default: outputs/pdf/)
            output_dir: Directory for TEI XML output (default: outputs/xml/)
            grobid_port: Port to expose GROBID on (default: 8070)
            container_name: Name for the Docker container
            db_path: Path to database (default: src/db/publications.db)
        """
        # Set directories
        if pdf_dir is None:
            pdf_dir = Path(__file__).parent.parent.parent / "outputs" / "pdf"
        if output_dir is None:
            output_dir = Path(__file__).parent.parent.parent / "outputs" / "xml"
        
        self.pdf_dir = Path(pdf_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Docker settings
        self.grobid_port = grobid_port
        self.container_name = container_name
        self.grobid_url = f"http://localhost:{grobid_port}"
        
        # Docker client
        try:
            self.docker_client = docker.from_env()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker not available: {e}")
        
        self.container = None
        
        # Statistics
        self.stats = {
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
        # Initialize database connection
        self.db = PublicationDatabase(db_path)
        
        print(f"✓ GROBID Converter initialized")
        print(f"  PDF directory: {self.pdf_dir.absolute()}")
        print(f"  Output directory: {self.output_dir.absolute()}")
        print(f"  GROBID URL: {self.grobid_url}")
        print(f"  Database: {self.db.db_path}")
    
    def _pull_grobid_image(self):
        """Pull GROBID Docker image if not available."""
        image_name = "lfoppiano/grobid:0.8.0"
        
        try:
            self.docker_client.images.get(image_name)
            print(f"✓ GROBID image already available: {image_name}")
        except ImageNotFound:
            print(f"Pulling GROBID image: {image_name} (this may take a while)...")
            try:
                self.docker_client.images.pull(image_name)
                print(f"✓ GROBID image pulled successfully")
            except Exception as e:
                raise RuntimeError(f"Failed to pull GROBID image: {e}")
    
    def start_grobid(self, wait_time: int = 30):
        """
        Start GROBID Docker container.
        
        Args:
            wait_time: Seconds to wait for GROBID to be ready
        """
        # Pull image if needed
        self._pull_grobid_image()
        
        # Check if container already exists
        try:
            self.container = self.docker_client.containers.get(self.container_name)
            
            if self.container.status == 'running':
                print(f"✓ GROBID container already running: {self.container_name}")
                # Verify it's responding
                if self._wait_for_grobid(timeout=5):
                    return
                else:
                    print("  Container running but not responding, restarting...")
                    self.container.restart()
            else:
                print(f"Starting existing GROBID container: {self.container_name}")
                self.container.start()
                
        except NotFound:
            # Container doesn't exist, create it
            print(f"Creating GROBID container: {self.container_name}")
            try:
                self.container = self.docker_client.containers.run(
                    "lfoppiano/grobid:0.8.0",
                    name=self.container_name,
                    ports={f'8070/tcp': self.grobid_port},
                    detach=True,
                    remove=False  # Keep container after stop for reuse
                )
                print(f"✓ GROBID container created: {self.container_name}")
            except APIError as e:
                raise RuntimeError(f"Failed to create GROBID container: {e}")
        
        # Wait for GROBID to be ready
        print(f"Waiting for GROBID to be ready (max {wait_time}s)...")
        if self._wait_for_grobid(timeout=wait_time):
            print("✓ GROBID is ready")
        else:
            raise RuntimeError("GROBID did not start within timeout period")
    
    def _wait_for_grobid(self, timeout: int = 30) -> bool:
        """
        Wait for GROBID service to be ready.
        
        Args:
            timeout: Maximum seconds to wait
            
        Returns:
            True if GROBID is ready, False otherwise
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    f"{self.grobid_url}/api/isalive",
                    timeout=2
                )
                if response.status_code == 200:
                    return True
            except requests.exceptions.RequestException:
                pass
            
            time.sleep(2)
        
        return False
    
    def stop_grobid(self):
        """Stop GROBID Docker container."""
        if self.container:
            print(f"Stopping GROBID container: {self.container_name}")
            try:
                self.container.stop()
                print("✓ GROBID container stopped")
            except Exception as e:
                print(f"Warning: Error stopping container: {e}")
    
    def remove_grobid(self):
        """Stop and remove GROBID Docker container."""
        if self.container:
            print(f"Removing GROBID container: {self.container_name}")
            try:
                self.container.stop()
                self.container.remove()
                print("✓ GROBID container removed")
            except Exception as e:
                print(f"Warning: Error removing container: {e}")
    
    def convert_pdf(
        self,
        pdf_path: Path,
        paper_id: str = None,
        overwrite: bool = False,
        delete_pdf: bool = False
    ) -> Tuple[bool, str]:
        """
        Convert a single PDF to TEI XML using GROBID.
        
        Args:
            pdf_path: Path to PDF file
            paper_id: Paper ID for output filename (uses pdf filename if None)
            overwrite: If True, re-convert even if XML exists
            delete_pdf: If True, delete PDF after successful conversion (default: False)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        if not pdf_path.exists():
            self.stats['failed'] += 1
            return False, f"PDF not found: {pdf_path}"
        
        # Determine output filename
        if paper_id is None:
            paper_id = pdf_path.stem
        
        output_path = self.output_dir / f"{paper_id}.tei.xml"
        
        # Check if already converted
        if output_path.exists() and not overwrite:
            self.stats['skipped'] += 1
            # Still delete PDF if requested
            if delete_pdf:
                try:
                    pdf_path.unlink()
                    return True, f"Already converted (PDF deleted): {paper_id}.tei.xml"
                except Exception as e:
                    return True, f"Already converted (PDF delete failed): {paper_id}.tei.xml"
            return True, f"Already converted: {paper_id}.tei.xml"
        
        # Convert using GROBID API
        try:
            with open(pdf_path, 'rb') as pdf_file:
                files = {'input': pdf_file}
                
                response = requests.post(
                    f"{self.grobid_url}/api/processFulltextDocument",
                    files=files,
                    timeout=300  # 5 minutes timeout for large PDFs
                )
                
                if response.status_code == 200:
                    # Save TEI XML
                    with open(output_path, 'w', encoding='utf-8') as xml_file:
                        xml_file.write(response.text)
                    
                    self.stats['successful'] += 1
                    file_size = output_path.stat().st_size / 1024  # KB
                    
                    # Delete PDF after successful conversion if requested
                    if delete_pdf:
                        try:
                            pdf_path.unlink()
                            return True, f"Converted & deleted: {paper_id}.tei.xml ({file_size:.1f} KB)"
                        except Exception as e:
                            return True, f"Converted (delete failed): {paper_id}.tei.xml ({file_size:.1f} KB)"
                    
                    return True, f"Converted: {paper_id}.tei.xml ({file_size:.1f} KB)"
                
                elif response.status_code == 503:
                    self.stats['failed'] += 1
                    return False, f"GROBID service unavailable (503)"
                
                else:
                    self.stats['failed'] += 1
                    return False, f"GROBID error: {response.status_code}"
                    
        except requests.exceptions.Timeout:
            self.stats['failed'] += 1
            return False, "Conversion timeout (>5 min)"
        
        except requests.exceptions.RequestException as e:
            self.stats['failed'] += 1
            return False, f"Request error: {str(e)}"
        
        except Exception as e:
            self.stats['failed'] += 1
            return False, f"Unexpected error: {str(e)}"
    
    def convert_pdfs(
        self,
        pdf_files: List[Path] = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = 0.1
    ) -> Dict:
        """
        Convert multiple PDFs to TEI XML with optional PDF deletion.
        
        Args:
            pdf_files: List of PDF paths (if None, uses all PDFs in pdf_dir)
            overwrite: If True, re-convert existing XMLs
            delete_pdf: If True, delete PDFs after successful conversion (default: False)
            delay: Delay between conversions in seconds
            
        Returns:
            Dictionary with conversion results and statistics
        """
        # Get PDF files
        if pdf_files is None:
            pdf_files = list(self.pdf_dir.glob("*.pdf"))
        
        if not pdf_files:
            print(f"No PDF files found in {self.pdf_dir}")
            return {'results': [], 'stats': self.stats}
        
        print(f"\nConverting {len(pdf_files)} PDFs to TEI XML...")
        if delete_pdf:
            print("⚠️  PDFs will be deleted after successful conversion")
        else:
            print("PDFs will be kept after conversion")
        
        results = []
        
        with tqdm(total=len(pdf_files), desc="Converting PDFs", unit="file") as pbar:
            for pdf_path in pdf_files:
                paper_id = pdf_path.stem  # Use filename without extension as paper ID
                
                success, message = self.convert_pdf(pdf_path, paper_id, overwrite, delete_pdf)
                
                results.append({
                    'paper_id': paper_id,
                    'pdf_path': str(pdf_path),
                    'success': success,
                    'message': message
                })
                
                pbar.set_postfix_str(message[:50])
                pbar.update(1)
                
                # Delay to avoid overwhelming GROBID
                if delay > 0:
                    time.sleep(delay)
        
        return {
            'results': results,
            'stats': self.stats.copy()
        }
    
    def convert_from_database(
        self,
        limit: int = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = 0.1
    ) -> Dict:
        """
        Convert PDFs from database that haven't been converted yet.
        
        Args:
            limit: Maximum number of PDFs to convert (None = all)
            overwrite: If True, re-convert existing XMLs
            delete_pdf: If True, delete PDFs after successful conversion
            delay: Delay between conversions in seconds
            
        Returns:
            Dictionary with conversion results
        """
        # Query PDFs that need conversion
        if overwrite:
            query = '''
                SELECT paperId, pdf_path
                FROM publications
                WHERE pdf_downloaded = 1 
                AND pdf_path IS NOT NULL
            '''
        else:
            query = '''
                SELECT paperId, pdf_path
                FROM publications
                WHERE pdf_downloaded = 1 
                AND pdf_path IS NOT NULL
                AND (xml_converted = 0 OR xml_converted IS NULL)
            '''
        
        if limit:
            query += f' LIMIT {limit}'
        
        self.db.cursor.execute(query)
        papers_to_convert = self.db.cursor.fetchall()
        
        if not papers_to_convert:
            print("No PDFs need conversion")
            return {'results': [], 'stats': self.stats}
        
        print(f"\nConverting {len(papers_to_convert)} PDFs from database...")
        if delete_pdf:
            print("WARNING: PDFs will be deleted after successful conversion")
        
        results = []
        
        with tqdm(total=len(papers_to_convert), desc="Converting PDFs", unit="file") as pbar:
            for row in papers_to_convert:
                paper_id = row[0]
                pdf_path = Path(row[1])
                
                # Check if PDF exists
                if not pdf_path.exists():
                    error_msg = f"PDF not found: {pdf_path}"
                    self.db.cursor.execute('''
                        UPDATE publications 
                        SET xml_converted = 0,
                            xml_conversion_error = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE paperId = ?
                    ''', (error_msg, paper_id))
                    self.db.conn.commit()
                    
                    results.append({
                        'paper_id': paper_id,
                        'pdf_path': str(pdf_path),
                        'success': False,
                        'message': error_msg
                    })
                    pbar.update(1)
                    continue
                
                # Convert PDF
                success, message = self.convert_pdf(pdf_path, paper_id, overwrite, delete_pdf)
                
                # Update database
                if success:
                    xml_path = str(self.output_dir / f"{paper_id}.tei.xml")
                    self.db.cursor.execute('''
                        UPDATE publications 
                        SET xml_converted = 1,
                            xml_conversion_date = CURRENT_TIMESTAMP,
                            xml_path = ?,
                            xml_conversion_error = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE paperId = ?
                    ''', (xml_path, paper_id))
                else:
                    self.db.cursor.execute('''
                        UPDATE publications 
                        SET xml_converted = 0,
                            xml_conversion_error = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE paperId = ?
                    ''', (message, paper_id))
                
                self.db.conn.commit()
                
                results.append({
                    'paper_id': paper_id,
                    'pdf_path': str(pdf_path),
                    'success': success,
                    'message': message
                })
                
                pbar.set_postfix_str(message[:50])
                pbar.update(1)
                
                if delay > 0:
                    time.sleep(delay)
        
        return {
            'results': results,
            'stats': self.stats.copy()
        }
    
    def close_db(self):
        """Close database connection."""
        if hasattr(self, 'db'):
            self.db.close()
    
    def get_statistics(self) -> Dict:
        """Get conversion statistics."""
        total = self.stats['successful'] + self.stats['failed']
        return {
            **self.stats,
            'success_rate': (self.stats['successful'] / total * 100) if total > 0 else 0
        }
    
    def print_statistics(self):
        """Print conversion statistics."""
        stats = self.get_statistics()
        print(f"\n{'='*60}")
        print("CONVERSION STATISTICS")
        print(f"{'='*60}")
        print(f"Successful:    {stats['successful']}")
        print(f"Failed:        {stats['failed']}")
        print(f"Skipped:       {stats['skipped']}")
        print(f"Success rate:  {stats['success_rate']:.1f}%")
        print(f"{'='*60}")
    
    def __enter__(self):
        """Context manager entry - starts GROBID."""
        self.start_grobid()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stops GROBID and closes DB."""
        self.stop_grobid()
        self.close_db()


# Example usage
if __name__ == "__main__":
    print("GROBID PDF to TEI XML Converter")
    print("=" * 60)
    
    try:
        # Initialize converter with database connection
        converter = GrobidConverter()
        
        # Start GROBID
        print("\nStarting GROBID container...")
        converter.start_grobid(wait_time=30)
        
        # Convert all PDFs from database
        print("\nConverting PDFs to TEI XML...")
        results = converter.convert_from_database(
            limit=None,  # Convert all available
            overwrite=False,
            delete_pdf=False,
            delay=0.1
        )
        
        # Print statistics
        converter.print_statistics()
        
        # Save results
        results_path = Path(__file__).parent.parent.parent / 'outputs' / 'metadata' / 'conversion_results.json'
        results_path.parent.mkdir(parents=True, exist_ok=True)
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {results_path}")
        
        # Stop GROBID
        print("\nStopping GROBID container...")
        converter.stop_grobid()
        converter.close_db()
        
        print("\n" + "=" * 60)
        print("✓ Conversion completed!")
        print(f"✓ XML files saved to: {converter.output_dir}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nMake sure Docker is installed and running.")