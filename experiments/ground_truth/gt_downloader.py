"""
GT Downloader
=============
Takes the list of papers that have an open-access PDF URL
(output of gt_fetcher) and downloads them into an isolated
experiment folder:  data/gt_experiment/pdf/

Does NOT touch the main pipeline DB.
Does NOT touch data/pdf/.
"""

import re
import sys
import time
import requests
from pathlib import Path
from tqdm import tqdm

# ── resolve src/ ──────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config import DATA_DIR

# ── isolated output dir ───────────────────────────────────────────────
GT_PDF_DIR = DATA_DIR / "gt_experiment" / "pdf"
GT_PDF_DIR.mkdir(parents=True, exist_ok=True)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ── helpers ───────────────────────────────────────────────────────────

def _safe_filename(paper_id: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "", paper_id) + ".pdf"


def _is_valid_pdf(path: Path) -> bool:
    try:
        return path.read_bytes()[:4] == b"%PDF"
    except Exception:
        return False


def _download_one(
    paper_id: str,
    url: str,
    output_dir: Path,
    overwrite: bool = False,
    retries: int = 3,
) -> tuple[bool, str, Path | None]:
    """
    Download a single PDF.

    Returns:
        (success, message, path_or_None)
    """
    dest = output_dir / _safe_filename(paper_id)

    if dest.exists() and not overwrite:
        if _is_valid_pdf(dest):
            return True, "already exists", dest
        dest.unlink()

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=60, stream=True)

            if r.status_code == 404:
                return False, "404 Not Found", None
            if r.status_code == 403:
                return False, "403 Forbidden", None
            r.raise_for_status()

            ct = r.headers.get("Content-Type", "").lower()
            if "pdf" not in ct and "octet-stream" not in ct:
                return False, f"unexpected content-type: {ct}", None

            dest.write_bytes(r.content)

            if not _is_valid_pdf(dest):
                dest.unlink(missing_ok=True)
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return False, "downloaded file is not a valid PDF", None

            size_mb = dest.stat().st_size / 1024 / 1024
            return True, f"{size_mb:.2f} MB", dest

        except requests.Timeout:
            if attempt < retries - 1:
                time.sleep(3)
                continue
            return False, "timeout", None

        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(3)
                continue
            return False, str(e), None

    return False, "max retries exceeded", None


# ── public API ────────────────────────────────────────────────────────

def download_papers(
    papers: list[dict],
    output_dir: Path = GT_PDF_DIR,
    delay: float = 0.5,
    overwrite: bool = False,
) -> dict:
    """
    Download PDFs for a list of SS paper dicts (must have openAccessPdf.url).

    Returns a results dict:
        {
            "downloaded": [ {paper_id, gt_id, path, title}, ... ],
            "failed":     [ {paper_id, gt_id, reason, title}, ... ],
            "skipped":    [ {paper_id, gt_id, title}, ... ],
            "stats": { successful, failed, skipped, total_mb }
        }
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    downloaded = []
    failed     = []
    skipped    = []
    total_mb   = 0.0

    print(f"\n{'='*70}")
    print(f"GT DOWNLOADER — {len(papers)} papers")
    print(f"Output: {output_dir}")
    print(f"{'='*70}")

    with tqdm(total=len(papers), desc="Downloading", unit="pdf") as pbar:
        for paper in papers:
            paper_id = paper.get("paperId", "")
            gt_id    = paper.get("_gt_id", "")
            title    = (paper.get("title") or "")[:60]
            oa       = paper.get("openAccessPdf") or {}
            url      = oa.get("url", "")

            if not url:
                skipped.append({"paper_id": paper_id, "gt_id": gt_id, "title": title})
                pbar.update(1)
                continue

            success, msg, path = _download_one(paper_id, url, output_dir, overwrite)

            if success and path:
                if msg == "already exists":
                    skipped.append({"paper_id": paper_id, "gt_id": gt_id,
                                    "title": title, "path": str(path)})
                else:
                    mb = float(msg.replace(" MB", ""))
                    total_mb += mb
                    downloaded.append({
                        "paper_id": paper_id,
                        "gt_id":    gt_id,
                        "title":    title,
                        "path":     str(path),
                        "size_mb":  mb,
                    })
            else:
                failed.append({
                    "paper_id": paper_id,
                    "gt_id":    gt_id,
                    "title":    title,
                    "reason":   msg,
                    "url":      url,
                })

            pbar.set_postfix_str(f"{gt_id} {'✓' if success else '✗'}")
            pbar.update(1)
            time.sleep(delay)

    stats = {
        "successful": len(downloaded),
        "failed":     len(failed),
        "skipped":    len(skipped),
        "total_mb":   round(total_mb, 2),
    }

    print(f"\n{'─'*70}")
    print(f"  Downloaded  : {stats['successful']}  ({stats['total_mb']} MB)")
    print(f"  Failed      : {stats['failed']}")
    print(f"  Skipped     : {stats['skipped']}")
    print(f"{'─'*70}")

    return {
        "downloaded": downloaded,
        "failed":     failed,
        "skipped":    skipped,
        "stats":      stats,
    }