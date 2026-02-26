import docker
from docker.errors import NotFound, APIError, ImageNotFound
import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import json
import sys

sys.path.append(str(Path(__file__).parent.parent))

from db.db import PublicationDatabase
from config import POSTGRES_DSN, PDF_DIR, XML_DIR
from utils.db_utils import update_xml_status


class GrobidConverter:
    """Converts PDFs to TEI XML via GROBID and updates DB via db.py methods."""

    def __init__(
        self,
        pdf_dir: str = None,
        output_dir: str = None,
        grobid_port: int = 8070,
        container_name: str = "grobid-server",
        db: "PublicationDatabase" = None,       # ← accept shared DB
    ):
        self.pdf_dir    = Path(pdf_dir)    if pdf_dir    else PDF_DIR
        self.output_dir = Path(output_dir) if output_dir else XML_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.grobid_port    = grobid_port
        self.container_name = container_name
        self.grobid_url     = f"http://localhost:{grobid_port}"

        try:
            self.docker_client = docker.from_env()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker not available: {e}")

        self.container = None
        self.stats = {'successful': 0, 'failed': 0, 'skipped': 0}

        # Use shared DB from pipeline — only create own if used standalone
        self._owns_db = db is None
        self.db       = db if db is not None else PublicationDatabase()

        print(f"✓ GROBID Converter initialized")
        print(f"  PDF directory   : {self.pdf_dir.absolute()}")
        print(f"  Output directory: {self.output_dir.absolute()}")
        print(f"  GROBID URL      : {self.grobid_url}")

    # ------------------------------------------------------------------
    # Docker helpers  (unchanged logic)
    # ------------------------------------------------------------------

    def _pull_grobid_image(self):
        image_name = "lfoppiano/grobid:0.8.0"
        try:
            self.docker_client.images.get(image_name)
            print(f"✓ GROBID image available: {image_name}")
        except ImageNotFound:
            print(f"Pulling GROBID image: {image_name}...")
            self.docker_client.images.pull(image_name)
            print("✓ GROBID image pulled")

    def start_grobid(self, wait_time: int = 30):
        self._pull_grobid_image()
        try:
            self.container = self.docker_client.containers.get(self.container_name)
            if self.container.status == 'running':
                if self._wait_for_grobid(timeout=5):
                    return
                self.container.restart()
            else:
                self.container.start()
        except NotFound:
            self.container = self.docker_client.containers.run(
                "lfoppiano/grobid:0.8.0",
                name=self.container_name,
                ports={'8070/tcp': self.grobid_port},
                detach=True,
                remove=False,
            )

        if not self._wait_for_grobid(timeout=wait_time):
            raise RuntimeError("GROBID did not start within timeout period")
        print("✓ GROBID is ready")

    def _wait_for_grobid(self, timeout: int = 30) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            try:
                r = requests.get(f"{self.grobid_url}/api/isalive", timeout=2)
                if r.status_code == 200:
                    return True
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        return False

    def stop_grobid(self):
        if self.container:
            try:
                self.container.stop()
                print("✓ GROBID container stopped")
            except Exception as e:
                print(f"Warning: {e}")

    # ------------------------------------------------------------------
    # Core conversion
    # ------------------------------------------------------------------

    def convert_pdf(
        self,
        pdf_path: Path,
        paper_id: str = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
    ) -> Tuple[bool, str]:
        if not pdf_path.exists():
            self.stats['failed'] += 1
            return False, f"PDF not found: {pdf_path}"

        paper_id    = paper_id or pdf_path.stem
        output_path = self.output_dir / f"{paper_id}.tei.xml"

        if output_path.exists() and not overwrite:
            self.stats['skipped'] += 1
            if delete_pdf:
                try:
                    pdf_path.unlink()
                except Exception:
                    pass
            return True, f"Already converted: {paper_id}.tei.xml"

        try:
            with open(pdf_path, 'rb') as f:
                response = requests.post(
                    f"{self.grobid_url}/api/processFulltextDocument",
                    files={'input': f},
                    timeout=300,
                )

            if response.status_code == 200:
                output_path.write_text(response.text, encoding='utf-8')
                self.stats['successful'] += 1
                size_kb = output_path.stat().st_size / 1024
                if delete_pdf:
                    try:
                        pdf_path.unlink()
                    except Exception:
                        pass
                return True, f"Converted: {paper_id}.tei.xml ({size_kb:.1f} KB)"

            elif response.status_code == 503:
                self.stats['failed'] += 1
                return False, "GROBID service unavailable (503)"
            else:
                self.stats['failed'] += 1
                return False, f"GROBID error: {response.status_code}"

        except requests.exceptions.Timeout:
            self.stats['failed'] += 1
            return False, "Conversion timeout (>5 min)"
        except Exception as e:
            self.stats['failed'] += 1
            return False, f"Error: {str(e)}"

    def convert_from_database(
        self,
        limit: int = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = 0.1,
    ) -> Dict:
        # Use db.py method — no raw SQL here
        papers = self.db.get_papers_needing_conversion(limit=limit)

        if not papers:
            print("No PDFs need conversion")
            return {'results': [], 'stats': self.stats}

        print(f"\nConverting {len(papers)} PDFs...")
        results = []

        with tqdm(total=len(papers), desc="Converting PDFs", unit="file") as pbar:
            for row in papers:
                paper_id = row['paperId']
                pdf_path = Path(row['pdf_path'])

                if not pdf_path.exists():
                    msg = f"PDF not found: {pdf_path}"
                    update_xml_status(self.db, paper_id, False, error=msg)
                    results.append({'paper_id': paper_id, 'success': False, 'message': msg})
                    pbar.update(1)
                    continue

                success, message = self.convert_pdf(pdf_path, paper_id, overwrite, delete_pdf)

                # Use centralised db_utils helper — no raw SQL
                xml_path = str(self.output_dir / f"{paper_id}.tei.xml") if success else None
                update_xml_status(self.db, paper_id, success, xml_path=xml_path, error=message)

                results.append({'paper_id': paper_id, 'success': success, 'message': message})
                pbar.set_postfix_str(message[:50])
                pbar.update(1)
                if delay > 0:
                    time.sleep(delay)

        return {'results': results, 'stats': self.stats.copy()}

    # ------------------------------------------------------------------
    # Stats / lifecycle
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict:
        total = self.stats['successful'] + self.stats['failed']
        return {**self.stats,
                'success_rate': (self.stats['successful'] / total * 100) if total > 0 else 0}

    def print_statistics(self):
        stats = self.get_statistics()
        print(f"\n{'='*60}\nCONVERSION STATISTICS\n{'='*60}")
        print(f"Successful:   {stats['successful']}")
        print(f"Failed:       {stats['failed']}")
        print(f"Skipped:      {stats['skipped']}")
        print(f"Success rate: {stats['success_rate']:.1f}%")
        print(f"{'='*60}")

    def close_db(self):
        """Only close DB if this instance owns it."""
        if self._owns_db:
            self.db.close()

    def __enter__(self):
        self.start_grobid()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_grobid()
        self.close_db()