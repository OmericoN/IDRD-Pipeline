"""
GT Runner  —  ground truth experiment entry point
==================================================

Runs a fully isolated pipeline on the ground truth CSV:

    1. FETCH    — search Semantic Scholar by title / DOI
    2. DOWNLOAD — download open-access PDFs to data/gt_experiment/pdf/
    3. CONVERT  — convert PDFs → TEI XML via GROBID to data/gt_experiment/xml/
    4. EXTRACT  — extract Markdown from TEI XML to data/gt_experiment/markdown/
    5. REPORT   — print + save a coverage report

Zero impact on:
    - The main database  (no DB connection opened)
    - data/pdf/          (writes to data/gt_experiment/pdf/)
    - data/xml/          (writes to data/gt_experiment/xml/)
    - data/markdown/     (writes to data/gt_experiment/markdown/)
    - main.py / IDRDPipeline

Usage
-----
    # Full run
    python experiments/ground_truth/gt_runner.py

    # Fetch + download + convert only (skip markdown extraction)
    python experiments/ground_truth/gt_runner.py --no-extract

    # Skip GROBID conversion
    python experiments/ground_truth/gt_runner.py --no-xml

    # Fetch only — no download, no convert, no extract
    python experiments/ground_truth/gt_runner.py --fetch-only

    # Show help
    python experiments/ground_truth/gt_runner.py --help
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# ── resolve src/ and experiments/ on sys.path ─────────────────────────
_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from config import DATA_DIR
from gt_fetcher    import fetch_all, load_csv, GT_CSV
from gt_downloader import download_papers, GT_PDF_DIR
from gt_report     import build_report, print_report, save_report

# ── isolated dirs ─────────────────────────────────────────────────────
GT_XML_DIR      = DATA_DIR / "gt_experiment" / "xml"
GT_MARKDOWN_DIR = DATA_DIR / "gt_experiment" / "markdown"
GT_XML_DIR.mkdir(parents=True, exist_ok=True)
GT_MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)


# ── GROBID conversion (isolated output) ───────────────────────────────

def _convert_pdfs(
    download_results: dict,
    output_dir: Path = GT_XML_DIR,
    delete_pdf: bool = False,
) -> dict:
    """
    Run GROBID on every successfully downloaded PDF.
    Calls convert_pdf() directly — never touches the main DB.
    """
    from ingestion.converter import GrobidConverter

    downloaded = download_results.get("downloaded", [])
    if not downloaded:
        print("\n  No PDFs to convert — skipping GROBID.")
        return {"results": [], "stats": {"successful": 0, "failed": 0, "skipped": 0}}

    print(f"\n{'='*70}")
    print(f"GT CONVERTER — {len(downloaded)} PDFs")
    print(f"Output: {output_dir}")
    print(f"{'='*70}")

    converter = GrobidConverter(output_dir=str(output_dir))
    results   = []
    stats     = {"successful": 0, "failed": 0, "skipped": 0}

    try:
        converter.start_grobid(wait_time=40)

        for entry in downloaded:
            pdf_path = Path(entry["path"])
            paper_id = entry["paper_id"]
            gt_id    = entry["gt_id"]

            success, msg = converter.convert_pdf(
                pdf_path   = pdf_path,
                paper_id   = paper_id,
                overwrite  = False,
                delete_pdf = delete_pdf,
            )

            status = "✓" if success else "✗"
            print(f"  {status} [{gt_id}] {entry['title'][:55]} — {msg}")

            results.append({
                "paper_id": paper_id,
                "gt_id":    gt_id,
                "success":  success,
                "message":  msg,
            })

            if success:
                stats["successful"] += 1
            else:
                stats["failed"] += 1

        print(f"\n{'─'*70}")
        print(f"  Converted : {stats['successful']}")
        print(f"  Failed    : {stats['failed']}")
        print(f"{'─'*70}")

    finally:
        converter.stop_grobid()
        converter.close_db()

    return {"results": results, "stats": stats}


# ── Markdown extraction (isolated output) ─────────────────────────────

def _extract_markdown(
    xml_dir: Path = GT_XML_DIR,
    output_dir: Path = GT_MARKDOWN_DIR,
) -> dict:
    """
    Run the extractor on every .tei.xml in xml_dir.
    Saves .md files to output_dir.
    Does NOT touch data/markdown/.
    """
    from ingestion.extractor import extract_markdown

    xml_files = sorted(xml_dir.glob("*.tei.xml"))

    if not xml_files:
        print("\n  No .tei.xml files found — skipping extraction.")
        return {"results": [], "stats": {"successful": 0, "failed": 0}}

    print(f"\n{'='*70}")
    print(f"GT EXTRACTOR — {len(xml_files)} XML files")
    print(f"Output: {output_dir}")
    print(f"{'='*70}")

    output_dir.mkdir(parents=True, exist_ok=True)

    results   = []
    stats     = {"successful": 0, "failed": 0}

    for xml_path in xml_files:
        # e.g. abc123def.tei.xml → abc123def.md
        stem        = xml_path.stem.replace(".tei", "")
        output_path = output_dir / f"{stem}.md"

        try:
            md = extract_markdown(xml_path)

            if not md.strip():
                raise ValueError("Empty markdown output")

            output_path.write_text(md, encoding="utf-8")
            size_kb = output_path.stat().st_size / 1024
            stats["successful"] += 1
            print(f"  ✓ {xml_path.name} → {output_path.name} ({size_kb:.1f} KB)")
            results.append({
                "xml":     xml_path.name,
                "md":      output_path.name,
                "size_kb": round(size_kb, 1),
                "success": True,
                "message": f"{size_kb:.1f} KB",
            })

        except Exception as e:
            stats["failed"] += 1
            print(f"  ✗ {xml_path.name} — {e}")
            results.append({
                "xml":     xml_path.name,
                "md":      None,
                "success": False,
                "message": str(e),
            })

    print(f"\n{'─'*70}")
    print(f"  Extracted : {stats['successful']}")
    print(f"  Failed    : {stats['failed']}")
    print(f"{'─'*70}")

    return {"results": results, "stats": stats}


# ── main runner ───────────────────────────────────────────────────────

def run(
    csv_path: Path = GT_CSV,
    convert: bool = True,
    extract: bool = True,
    fetch_only: bool = False,
    delay_fetch: float = 0.2,
    delay_download: float = 0.5,
    delete_pdfs: bool = False,
):
    start = time.time()

    print(f"\n{'='*70}")
    print("GROUND TRUTH EXPERIMENT RUNNER")
    print(f"{'='*70}")
    print(f"  CSV           : {csv_path}")
    print(f"  Convert XML   : {convert}")
    print(f"  Extract MD    : {extract}")
    print(f"  Fetch only    : {fetch_only}")
    print(f"  PDF dir       : {GT_PDF_DIR}")
    print(f"  XML dir       : {GT_XML_DIR}")
    print(f"  Markdown dir  : {GT_MARKDOWN_DIR}")
    print(f"  Started       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")

    # ── 1. FETCH ──────────────────────────────────────────────────────
    found_with_pdf, found_no_pdf, not_found = fetch_all(
        csv_path=csv_path,
        delay=delay_fetch,
    )
    total_in_csv = len(load_csv(csv_path))

    if fetch_only or not found_with_pdf:
        if not found_with_pdf:
            print("\n  No papers with downloadable PDFs found — stopping.")

        report = build_report(
            found_with_pdf     = found_with_pdf,
            found_no_pdf       = found_no_pdf,
            not_found          = not_found,
            download_results   = {"stats": {"successful": 0, "failed": 0, "skipped": 0}},
            conversion_results = None,
            extraction_results = None,
            total_in_csv       = total_in_csv,
            elapsed_seconds    = time.time() - start,
        )
        print_report(report)
        save_report(report)
        return

    # ── 2. DOWNLOAD ───────────────────────────────────────────────────
    download_results = download_papers(
        papers     = found_with_pdf,
        output_dir = GT_PDF_DIR,
        delay      = delay_download,
    )

    # ── 3. CONVERT ────────────────────────────────────────────────────
    conversion_results = None
    if convert and download_results["stats"]["successful"] > 0:
        conversion_results = _convert_pdfs(
            download_results = download_results,
            output_dir       = GT_XML_DIR,
            delete_pdf       = delete_pdfs,
        )

    # ── 4. EXTRACT ────────────────────────────────────────────────────
    extraction_results = None
    if extract and (conversion_results or {}).get("stats", {}).get("successful", 0) > 0:
        extraction_results = _extract_markdown(
            xml_dir    = GT_XML_DIR,
            output_dir = GT_MARKDOWN_DIR,
        )

    # ── 5. REPORT ─────────────────────────────────────────────────────
    report = build_report(
        found_with_pdf     = found_with_pdf,
        found_no_pdf       = found_no_pdf,
        not_found          = not_found,
        download_results   = download_results,
        conversion_results = conversion_results,
        extraction_results = extraction_results,
        total_in_csv       = total_in_csv,
        elapsed_seconds    = time.time() - start,
    )
    print_report(report)
    save_report(report)


# ── CLI ───────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python experiments/ground_truth/gt_runner.py",
        description="Ground Truth Experiment Runner — isolated pipeline test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES
────────
  Full run (fetch + download + convert + extract):
      python experiments/ground_truth/gt_runner.py

  Skip markdown extraction:
      python experiments/ground_truth/gt_runner.py --no-extract

  Skip GROBID conversion (and extraction):
      python experiments/ground_truth/gt_runner.py --no-xml

  Fetch metadata only:
      python experiments/ground_truth/gt_runner.py --fetch-only

  Custom CSV:
      python experiments/ground_truth/gt_runner.py --csv data/ground_truth/temp.csv

  Delete PDFs after conversion:
      python experiments/ground_truth/gt_runner.py --delete-pdfs

  Re-extract markdown from existing XMLs (no fetch/download/convert):
      python experiments/ground_truth/gt_runner.py --extract-only
        """,
    )
    parser.add_argument(
        "--csv", type=str, default=None,
        help=f"Path to ground truth CSV (default: {GT_CSV})",
    )
    parser.add_argument(
        "--no-xml", action="store_true",
        help="Skip GROBID conversion — fetch + download only",
    )
    parser.add_argument(
        "--no-extract", action="store_true",
        help="Skip markdown extraction",
    )
    parser.add_argument(
        "--fetch-only", action="store_true",
        help="Only fetch metadata from Semantic Scholar — no download, convert, or extract",
    )
    parser.add_argument(
        "--extract-only", action="store_true",
        help="Only run extractor on existing XMLs in data/gt_experiment/xml/",
    )
    parser.add_argument(
        "--delete-pdfs", action="store_true",
        help="Delete PDFs after successful GROBID conversion",
    )
    parser.add_argument(
        "--fetch-delay", type=float, default=0.2,
        help="Seconds between Semantic Scholar requests (default: 0.2)",
    )
    parser.add_argument(
        "--download-delay", type=float, default=0.5,
        help="Seconds between PDF downloads (default: 0.5)",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args   = parser.parse_args()

    # ── extract-only shortcut ──────────────────────────────────────────
    if args.extract_only:
        _extract_markdown(xml_dir=GT_XML_DIR, output_dir=GT_MARKDOWN_DIR)
        sys.exit(0)

    run(
        csv_path       = Path(args.csv) if args.csv else GT_CSV,
        convert        = not args.no_xml,
        extract        = not args.no_extract,
        fetch_only     = args.fetch_only,
        delay_fetch    = args.fetch_delay,
        delay_download = args.download_delay,
        delete_pdfs    = args.delete_pdfs,
    )