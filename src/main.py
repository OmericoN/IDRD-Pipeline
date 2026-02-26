"""
IDRD Pipeline - Main Orchestrator
==================================
Pipeline Flow:
    1. Fetch    — Search & store papers from Semantic Scholar
    2. Download — Download open-access PDFs
    3. Convert  — Convert PDFs to TEI XML via GROBID (Docker)
    4. Extract  — Extract Markdown from TEI XML
    5. Features — LLM feature extraction  [Phase 3]
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent))

from pubfetcher.client import SemanticScholarClient
from utils.dict_parser import PaperDictParser
from db.db import PublicationDatabase
from ingestion.downloader import PDFDownloader
from ingestion.converter import GrobidConverter
from ingestion.extractor import extract_markdown
from utils.db_utils import (
    print_download_status,
    print_conversion_status,
    sync_existing_pdfs,
)
from config import PDF_DIR, XML_DIR, MARKDOWN_DIR, RUNS_DIR


class IDRDPipeline:
    """Main pipeline orchestrator for IDRD paper processing."""

    def __init__(self):
        self.db         = PublicationDatabase()
        self.parser     = PaperDictParser()
        self.start_time = datetime.now()

        run_label    = self.start_time.strftime('%Y-%m-%d_%H-%M-%S')
        self.run_dir = RUNS_DIR / run_label
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / 'metadata').mkdir(exist_ok=True)

        print("\n" + "=" * 70)
        print("IDRD PIPELINE INITIALIZED")
        print("=" * 70)
        print(f"  Start time : {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Run logs   : {self.run_dir}")
        print(f"  PDF data   : {PDF_DIR}")
        print(f"  XML data   : {XML_DIR}")
        print(f"  Markdown   : {MARKDOWN_DIR}")
        print("=" * 70)

    # ------------------------------------------------------------------
    # Step 1 — Fetch
    # ------------------------------------------------------------------

    def step_1_fetch_papers(
        self,
        query: str,
        limit: int = 100,
        open_access_only: bool = True,
        fields_of_study: str = None,
    ) -> int:
        print("\n" + "=" * 70)
        print("STEP 1: FETCHING PAPERS FROM SEMANTIC SCHOLAR")
        print("=" * 70)
        print(f"  Query            : '{query}'")
        print(f"  Limit            : {limit}")
        print(f"  Open access only : {open_access_only}")
        if fields_of_study:
            print(f"  Fields of study  : {fields_of_study}")
        print("=" * 70)

        client = SemanticScholarClient()
        papers = client.search_papers(
            query=query,
            limit=limit,
            open_access_pdf=open_access_only,
            fields_of_study=fields_of_study,
        )

        if not papers:
            print("\n  No papers found matching criteria.")
            return 0

        self.parser.parse_papers(papers)
        json_path = self.run_dir / 'metadata' / 'retrieved_results.json'
        self.parser.to_json(str(json_path))

        count = self.db.insert_publications(papers)

        open_access_count = sum(1 for p in papers if (p.get('openAccessPdf') or {}).get('url'))
        papers_with_doi   = sum(1 for p in papers if (p.get('externalIds') or {}).get('DOI'))

        print("\n" + "-" * 70)
        print("STEP 1 SUMMARY")
        print("-" * 70)
        print(f"  Fetched          : {len(papers)}")
        print(f"  Saved to DB      : {count}")
        print(f"  With PDF URLs    : {open_access_count}")
        print(f"  With DOI         : {papers_with_doi}")
        print(f"  Log              : {json_path}")
        print("-" * 70)

        return count

    # ------------------------------------------------------------------
    # Step 2 — Download
    # ------------------------------------------------------------------

    def step_2_download_pdfs(
        self,
        limit: int = None,
        overwrite: bool = False,
        delay: float = 0.5,
    ) -> dict:
        print("\n" + "=" * 70)
        print("STEP 2: DOWNLOADING PDFs")
        print("=" * 70)
        print(f"  Limit            : {limit or 'All available'}")
        print(f"  Overwrite        : {overwrite}")
        print(f"  Delay (s)        : {delay}")
        print(f"  Output           : {PDF_DIR}")
        print("=" * 70)

        downloader = PDFDownloader(db=self.db)

        try:
            print_download_status(self.db, downloader.output_dir)
            sync_existing_pdfs(self.db, downloader.output_dir)

            results = downloader.download_from_database(
                limit=limit,
                overwrite=overwrite,
                delay=delay,
            )

            downloader.print_statistics()
            print("\n" + "-" * 70)
            print("STEP 2 COMPLETE")
            print("-" * 70)
            print_download_status(self.db, downloader.output_dir)

            self._save_results(results, 'download_results.json')
            return results

        finally:
            downloader.close()

    # ------------------------------------------------------------------
    # Step 3 — Convert
    # ------------------------------------------------------------------

    def step_3_convert_to_xml(
        self,
        limit: int = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = 0.1,
    ) -> dict:
        print("\n" + "=" * 70)
        print("STEP 3: CONVERTING PDFs TO TEI XML")
        print("=" * 70)
        print(f"  Limit            : {limit or 'All available'}")
        print(f"  Overwrite        : {overwrite}")
        print(f"  Delete PDFs      : {delete_pdf}")
        print(f"  Delay (s)        : {delay}")
        print(f"  Output           : {XML_DIR}")
        print("=" * 70)

        converter = GrobidConverter(db=self.db)

        try:
            print_conversion_status(self.db, converter.output_dir)

            print("\n" + "-" * 70)
            print("Starting GROBID Docker container...")
            print("-" * 70)
            converter.start_grobid(wait_time=30)

            results = converter.convert_from_database(
                limit=limit,
                overwrite=overwrite,
                delete_pdf=delete_pdf,
                delay=delay,
            )

            converter.print_statistics()
            print("\n" + "-" * 70)
            print("STEP 3 COMPLETE")
            print("-" * 70)
            print_conversion_status(self.db, converter.output_dir)

            self._save_results(results, 'conversion_results.json')
            return results

        except Exception as e:
            print(f"\n  Error in conversion step: {e}")
            raise
        finally:
            converter.stop_grobid()
            converter.close_db()

    # ------------------------------------------------------------------
    # Step 4 — Extract Markdown
    # ------------------------------------------------------------------

    def step_4_extract_markdown(
        self,
        limit: int = None,
        overwrite: bool = False,
    ) -> dict:
        print("\n" + "=" * 70)
        print("STEP 4: EXTRACTING MARKDOWN FROM TEI XML")
        print("=" * 70)
        print(f"  Limit            : {limit or 'All available'}")
        print(f"  Overwrite        : {overwrite}")
        print(f"  Input            : {XML_DIR}")
        print(f"  Output           : {MARKDOWN_DIR}")
        print("=" * 70)

        MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)

        xml_files = sorted(XML_DIR.glob("*.tei.xml"))
        if limit:
            xml_files = xml_files[:limit]

        if not xml_files:
            print("\n  No .tei.xml files found — skipping extraction.")
            return {"results": [], "stats": {"successful": 0, "failed": 0, "skipped": 0}}

        print(f"\n  Found {len(xml_files)} XML files to process\n")

        results   = []
        stats     = {"successful": 0, "failed": 0, "skipped": 0}

        for xml_path in xml_files:
            stem        = xml_path.stem.replace(".tei", "")
            output_path = MARKDOWN_DIR / f"{stem}.md"

            if output_path.exists() and not overwrite:
                stats["skipped"] += 1
                results.append({
                    "xml":     xml_path.name,
                    "md":      output_path.name,
                    "success": True,
                    "message": "skipped — already exists",
                })
                continue

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
                    "success": True,
                    "message": f"{size_kb:.1f} KB",
                })

                # mark in DB
                self.db.cursor.execute(
                    'UPDATE publications SET sections_extracted = TRUE, updated_at = CURRENT_TIMESTAMP WHERE pdf_path LIKE %s',
                    (f"%{stem}%",)
                )
                self.db.commit()

            except Exception as e:
                stats["failed"] += 1
                print(f"  ✗ {xml_path.name} — {e}")
                results.append({
                    "xml":     xml_path.name,
                    "md":      None,
                    "success": False,
                    "message": str(e),
                })

        print("\n" + "-" * 70)
        print("STEP 4 COMPLETE")
        print("-" * 70)
        print(f"  Extracted : {stats['successful']}")
        print(f"  Skipped   : {stats['skipped']}")
        print(f"  Failed    : {stats['failed']}")
        print("-" * 70)

        self._save_results({"results": results, "stats": stats}, 'extraction_results.json')
        return {"results": results, "stats": stats}

    # ------------------------------------------------------------------
    # Step 5 — Placeholder
    # ------------------------------------------------------------------

    def step_5_extract_features(self):
        print("\n" + "=" * 70)
        print("STEP 5: LLM FEATURE EXTRACTION  [NOT YET IMPLEMENTED]")
        print("=" * 70)

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run_full_pipeline(
        self,
        query: str,
        limit: int = 100,
        open_access_only: bool = True,
        convert_to_xml: bool = True,
        extract_markdown: bool = True,
        delete_pdfs_after_conversion: bool = False,
    ):
        print("\n" + "=" * 70)
        print("RUNNING FULL IDRD PIPELINE")
        print("=" * 70)
        print(f"  Query    : '{query}'")
        print(f"  Limit    : {limit}")
        print(f"  Run logs : {self.run_dir}")
        print("=" * 70)

        try:
            papers_fetched = self.step_1_fetch_papers(
                query=query,
                limit=limit,
                open_access_only=open_access_only,
            )

            if papers_fetched == 0:
                print("\n  No papers fetched — pipeline stopped.")
                return

            download_results = self.step_2_download_pdfs()

            if download_results['stats']['successful'] == 0:
                print("\n  No PDFs downloaded — skipping XML conversion.")
                return

            if convert_to_xml:
                conversion_results = self.step_3_convert_to_xml(
                    delete_pdf=delete_pdfs_after_conversion,
                )

                if extract_markdown and conversion_results['stats']['successful'] > 0:
                    self.step_4_extract_markdown()

            self._print_final_summary()

        except KeyboardInterrupt:
            print("\n\n  Pipeline interrupted by user.")
        except Exception as e:
            print(f"\n\n  Pipeline error: {e}")
            raise
        finally:
            self.cleanup()

    # ------------------------------------------------------------------
    # Status / Reset
    # ------------------------------------------------------------------

    def print_status(self):
        status = self.db.get_pipeline_status()

        print("\n" + "=" * 70)
        print("PIPELINE STATUS")
        print("=" * 70)
        print(f"  Total papers          : {status['total_papers']}")
        print(f"  PDFs downloaded       : {status['pdf_downloaded']}")
        print(f"  Converted to XML      : {status['xml_converted']}")
        print(f"  Sections extracted    : {status['sections_extracted']}")
        print(f"  Features extracted    : {status['features_extracted']}")
        print(f"  Download errors       : {status['pdf_errors']}")
        print(f"  Conversion errors     : {status['xml_errors']}")
        print("=" * 70)

        return status

    def reset_pipeline(self, reset_type: str = "status"):
        if reset_type == "status":
            self.db.reset_pipeline_status()
        elif reset_type == "full":
            print("\n" + "=" * 70)
            print("⚠  WARNING: FULL DATABASE RESET")
            print("=" * 70)
            confirm1 = input("\n  Type 'yes' to continue: ")
            if confirm1.lower() != 'yes':
                print("  Reset cancelled.")
                return
            confirm2 = input("  Type your full name 'Omer Nidam' to confirm: ")
            if confirm2 == "Omer Nidam":
                self.db.reset_database(confirm=True)
                print("\n  ✓ Database fully reset.")
            else:
                print("\n  Incorrect name — reset cancelled.")
        else:
            print(f"  Unknown reset type: '{reset_type}'. Use 'status' or 'full'.")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _save_results(self, results: dict, filename: str):
        out = self.run_dir / 'metadata' / filename
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        print(f"  Log saved to: {out}")

    def _print_final_summary(self):
        end_time = datetime.now()
        duration = end_time - self.start_time
        status   = self.db.get_pipeline_status()

        print("\n" + "=" * 70)
        print("PIPELINE COMPLETE")
        print("=" * 70)
        print(f"  Total papers          : {status['total_papers']}")
        print(f"  PDFs downloaded       : {status['pdf_downloaded']}")
        print(f"  Converted to XML      : {status['xml_converted']}")
        print(f"  Sections extracted    : {status['sections_extracted']}")
        print(f"  Download errors       : {status['pdf_errors']}")
        print(f"  Conversion errors     : {status['xml_errors']}")
        print(f"\n  Started  : {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Finished : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Duration : {duration}")
        print(f"  Run logs : {self.run_dir}")
        print(f"  PDF data : {PDF_DIR}")
        print(f"  XML data : {XML_DIR}")
        print(f"  Markdown : {MARKDOWN_DIR}")
        print("=" * 70)

    def cleanup(self):
        try:
            self.db.close()
        except Exception:
            pass
        print("\n  Pipeline resources cleaned up.")


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python src/main.py",
        description="IDRD Pipeline — Research Paper Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
MODES
─────
  Full pipeline:      python src/main.py --query "Transformers" --limit 50
  Fetch only:         python src/main.py --query "Transformers" --fetch-only
  Download only:      python src/main.py --download-only
  Convert only:       python src/main.py --convert-only
  Extract only:       python src/main.py --extract-only
  Status:             python src/main.py --status
  Reset tracking:     python src/main.py --reset status
  Full DB reset:      python src/main.py --reset full
        """,
    )

    mode = parser.add_argument_group("Pipeline modes")
    mode.add_argument("--fetch-only",    action="store_true")
    mode.add_argument("--download-only", action="store_true")
    mode.add_argument("--convert-only",  action="store_true")
    mode.add_argument("--extract-only",  action="store_true",
                      help="Run markdown extraction on existing XMLs in data/xml/")
    mode.add_argument("--status",        action="store_true")
    mode.add_argument("--reset", choices=["status", "full"], metavar="{status|full}")

    fetch = parser.add_argument_group("Fetch options")
    fetch.add_argument("--query",           type=str)
    fetch.add_argument("--limit",           type=int, default=100)
    fetch.add_argument("--fields-of-study", type=str)
    fetch.add_argument("--all-access",      action="store_true")

    dl = parser.add_argument_group("Download options")
    dl.add_argument("--dl-limit",     type=int,   default=None)
    dl.add_argument("--dl-overwrite", action="store_true")
    dl.add_argument("--dl-delay",     type=float, default=0.5)

    cv = parser.add_argument_group("Convert options")
    cv.add_argument("--cv-limit",     type=int,   default=None)
    cv.add_argument("--cv-overwrite", action="store_true")
    cv.add_argument("--cv-delay",     type=float, default=0.1)
    cv.add_argument("--delete-pdfs",  action="store_true")
    cv.add_argument("--no-xml",       action="store_true")

    ex = parser.add_argument_group("Extract options")
    ex.add_argument("--ex-limit",     type=int,  default=None)
    ex.add_argument("--ex-overwrite", action="store_true")
    ex.add_argument("--no-extract",   action="store_true",
                    help="Skip markdown extraction step in full pipeline")

    return parser


def main():
    parser   = build_parser()
    args     = parser.parse_args()
    pipeline = IDRDPipeline()

    try:
        if args.status:
            pipeline.print_status()
            return

        if args.reset:
            pipeline.reset_pipeline(args.reset)
            return

        if args.fetch_only:
            if not args.query:
                parser.error("--query is required with --fetch-only")
            pipeline.step_1_fetch_papers(
                query=args.query,
                limit=args.limit,
                open_access_only=not args.all_access,
                fields_of_study=args.fields_of_study,
            )
            return

        if args.download_only:
            pipeline.step_2_download_pdfs(
                limit=args.dl_limit,
                overwrite=args.dl_overwrite,
                delay=args.dl_delay,
            )
            return

        if args.convert_only:
            pipeline.step_3_convert_to_xml(
                limit=args.cv_limit,
                overwrite=args.cv_overwrite,
                delete_pdf=args.delete_pdfs,
                delay=args.cv_delay,
            )
            return

        if args.extract_only:
            pipeline.step_4_extract_markdown(
                limit=args.ex_limit,
                overwrite=args.ex_overwrite,
            )
            return

        if not args.query:
            parser.error("--query is required to run the full pipeline")

        pipeline.run_full_pipeline(
            query=args.query,
            limit=args.limit,
            open_access_only=not args.all_access,
            convert_to_xml=not args.no_xml,
            extract_markdown=not args.no_extract,
            delete_pdfs_after_conversion=args.delete_pdfs,
        )

    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()