"""
IDRD Pipeline - Main Orchestrator
==================================
Pipeline Flow:
    1. Fetch    — Search & store papers from Semantic Scholar
    2. Download — Download open-access PDFs
    3. Convert  — Convert PDFs to TEI XML via GROBID (Docker)
    4. Render   — Render Markdown from TEI XML
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
from db.db import IDRDDatabase
from ingestion.downloader import PDFDownloader
from ingestion.converter import GrobidConverter
from ingestion.renderer import extract_markdown
from utils.db_utils import (
    print_download_status,
    print_conversion_status,
    sync_existing_pdfs,
)
from config import PDF_DIR, XML_DIR, MARKDOWN_DIR, RUNS_DIR


class IDRDPipeline:
    """Main pipeline orchestrator for IDRD paper processing."""

    def __init__(self):
        self.db         = IDRDDatabase()
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
    # Step 2 — Download PDFs (Results-Based API)
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

        downloader = PDFDownloader(output_dir=str(PDF_DIR), db=None)  # No DB coupling

        try:
            print_download_status(self.db, downloader.output_dir)
            sync_existing_pdfs(self.db, downloader.output_dir)

            # Get papers from DB (data source)
            papers = self.db.get_papers_needing_download(limit=limit)
            
            if not papers:
                print("\n  No papers need PDF downloads.")
                return {"results": [], "stats": {"successful": 0, "failed": 0, "skipped": 0}}

            # Download PDFs (no DB coupling in component)
            results = downloader.download_papers(
                papers=papers,
                paper_id_key='paperId',
                url_key='url',
                overwrite=overwrite,
                delay=delay,
            )

            # Persist results to DB (separate persistence step)
            from utils.db_utils import persist_download_results
            persist_download_results(self.db, results)

            # Calculate stats
            stats = {
                "successful": sum(1 for r in results if r.success and r.message != "skipped — already exists"),
                "failed": sum(1 for r in results if not r.success),
                "skipped": sum(1 for r in results if r.success and r.message == "skipped — already exists"),
            }

            print("\n" + "-" * 70)
            print("STEP 2 COMPLETE")
            print("-" * 70)
            print(f"  Successful : {stats['successful']}")
            print(f"  Failed     : {stats['failed']}")
            print(f"  Skipped    : {stats['skipped']}")
            print("-" * 70)
            print_download_status(self.db, downloader.output_dir)

            output = {"results": [vars(r) for r in results], "stats": stats}
            self._save_results(output, 'download_results.json')
            return output

        finally:
            downloader.close()

    # ------------------------------------------------------------------
    # Step 3 — Convert to XML (Results-Based API)
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

        # Create converter (no DB coupling)
        converter = GrobidConverter(output_dir=str(XML_DIR), db=None)

        try:
            print_conversion_status(self.db, converter.output_dir)

            # Start GROBID automatically (pulls image, starts container, waits for ready)
            print("\n" + "-" * 70)
            print("Starting GROBID Docker container...")
            print("-" * 70)
            converter.start_grobid(wait_time=30)

            # Get papers from DB (data source)
            papers = self.db.get_papers_needing_conversion(limit=limit)
            
            if not papers:
                print("\n  No papers need conversion.")
                return {"results": [], "stats": {"successful": 0, "failed": 0, "skipped": 0}}

            # Convert PDFs (no DB coupling in component)
            results = converter.convert_papers(
                papers=papers,
                paper_id_key='paperId',
                pdf_path_key='pdf_path',
                overwrite=overwrite,
                delete_pdf=delete_pdf,
                delay=delay,
            )

            # Persist results to DB (separate persistence step)
            from utils.db_utils import persist_conversion_results
            persist_conversion_results(self.db, results)

            # Calculate stats
            stats = {
                "successful": sum(1 for r in results if r.success and "already converted" not in r.message.lower()),
                "failed": sum(1 for r in results if not r.success),
                "skipped": sum(1 for r in results if r.success and "already converted" in r.message.lower()),
            }

            print("\n" + "-" * 70)
            print("STEP 3 COMPLETE")
            print("-" * 70)
            print(f"  Successful : {stats['successful']}")
            print(f"  Failed     : {stats['failed']}")
            print(f"  Skipped    : {stats['skipped']}")
            print("-" * 70)
            print_conversion_status(self.db, converter.output_dir)

            output = {"results": [vars(r) for r in results], "stats": stats}
            self._save_results(output, 'conversion_results.json')
            return output

        except Exception as e:
            print(f"\n  Error in conversion step: {e}")
            raise
        finally:
            converter.stop_grobid()
            # Note: converter.close_db() not needed - converter doesn't own DB connection

    # ------------------------------------------------------------------
    # Step 4 — Render Markdown (Results-Based API)
    # ------------------------------------------------------------------

    def step_4_render_markdown(
        self,
        limit: int = None,
        overwrite: bool = False,
    ) -> dict:
        print("\n" + "=" * 70)
        print("STEP 4: RENDERING MARKDOWN FROM TEI XML")
        print("=" * 70)
        print(f"  Limit            : {limit or 'All available'}")
        print(f"  Overwrite        : {overwrite}")
        print(f"  Input            : {XML_DIR}")
        print(f"  Output           : {MARKDOWN_DIR}")
        print("=" * 70)

        MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)

        # Get papers from DB (data source)
        papers = self.db.get_papers_needing_rendering(limit=limit)
        
        if not papers:
            print("\n  No papers need markdown rendering.")
            return {"results": [], "stats": {"successful": 0, "failed": 0, "skipped": 0}}

        print(f"\n  Found {len(papers)} papers to render\n")

        # Render markdown (no DB coupling - uses render_to_markdown)
        from ingestion.renderer import render_to_markdown
        
        results = []
        stats = {"successful": 0, "failed": 0, "skipped": 0}

        for paper in papers:
            paper_id = paper['paperId']
            xml_path = Path(paper['xml_path'])
            md_path = MARKDOWN_DIR / f"{paper_id}.md"

            if md_path.exists() and not overwrite:
                stats["skipped"] += 1
                results.append({
                    "paper_id": paper_id,
                    "xml": xml_path.name,
                    "md": md_path.name,
                    "success": True,
                    "message": "skipped — already exists",
                })
                continue

            try:
                # Use results-based renderer function
                result = render_to_markdown(xml_path, md_path)
                
                stats["successful"] += 1
                size_kb = md_path.stat().st_size / 1024
                print(f"  [OK] {xml_path.name} -> {md_path.name} ({size_kb:.1f} KB)")
                
                results.append({
                    "paper_id": paper_id,
                    "xml": xml_path.name,
                    "md": md_path.name,
                    "success": result.success,
                    "message": result.message,
                    "sections_extracted": result.sections_extracted,
                    "references_count": result.references_count,
                })

                # Persist to DB (separate step)
                self.db.cursor.execute(
                    '''UPDATE publications 
                       SET sections_extracted = TRUE, 
                           updated_at = CURRENT_TIMESTAMP 
                       WHERE "paperId" = %s''',
                    (paper_id,)
                )
                self.db.commit()

            except Exception as e:
                stats["failed"] += 1
                error_msg = f"Failed to render {xml_path.name}: {type(e).__name__}: {str(e)}"
                print(f"  ✗ {error_msg}")
                results.append({
                    "paper_id": paper_id,
                    "xml": xml_path.name,
                    "md": None,
                    "success": False,
                    "message": error_msg,
                })

        print("\n" + "-" * 70)
        print("STEP 4 COMPLETE")
        print("-" * 70)
        print(f"  Successful : {stats['successful']}")
        print(f"  Skipped    : {stats['skipped']}")
        print(f"  Failed     : {stats['failed']}")
        print("-" * 70)

        output = {"results": results, "stats": stats}
        self._save_results(output, 'rendering_results.json')
        return output

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
        render_markdown: bool = True,
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

                if render_markdown and conversion_results['stats']['successful'] > 0:
                    self.step_4_render_markdown()

            self._print_final_summary()

        except KeyboardInterrupt:
            print("\n\n  Pipeline interrupted by user.")
        except Exception as e:
            print(f"\n\n  Pipeline error: {e}")
            raise
        finally:
            self.cleanup()

    # ------------------------------------------------------------------
    # Resume
    # ------------------------------------------------------------------

    def resume_pipeline(
        self,
        query: str = None,
        limit: int = 100,
        open_access_only: bool = True,
        delete_pdfs_after_conversion: bool = False,
    ):
        """Resume pipeline from the last incomplete stage.
        
        Intelligently detects which stage needs work by comparing counts:
        - If total == 0: Fetch papers (Step 1)
        - If downloaded < total: Download PDFs (Step 2)
        - If converted < downloaded: Convert to XML (Step 3)
        - If extracted < converted: Render markdown (Step 4)
        """
        print("\n" + "=" * 70)
        print("RESUMING IDRD PIPELINE")
        print("=" * 70)

        status     = self.db.get_pipeline_status()
        total      = status['total_papers']
        downloaded = status['pdf_downloaded']
        converted  = status['xml_converted']
        extracted  = status['sections_extracted']

        print(f"  Total papers       : {total}")
        print(f"  PDFs downloaded    : {downloaded}")
        print(f"  Converted to XML   : {converted}")
        print(f"  Sections extracted : {extracted}")
        print("=" * 70)

        try:
            # Query database to see what ACTUALLY needs to be done (not just count differences)
            papers_needing_download = self.db.get_papers_needing_download(limit=1)
            papers_needing_conversion = self.db.get_papers_needing_conversion(limit=1)
            papers_needing_rendering = self.db.get_papers_needing_rendering(limit=1)

            # Run ONLY the next step based on what's actually available in DB
            if len(papers_needing_download) > 0:
                all_needing_download = self.db.get_papers_needing_download()
                count = len(all_needing_download)
                print(f"\n  -> Next Step: DOWNLOAD PDFs ({count} papers need downloads)")
                self.step_2_download_pdfs()

            elif len(papers_needing_conversion) > 0:
                all_needing_conversion = self.db.get_papers_needing_conversion()
                count = len(all_needing_conversion)
                print(f"\n  -> Next Step: CONVERT to XML ({count} PDFs need conversion)")
                self.step_3_convert_to_xml()

            elif len(papers_needing_rendering) > 0:
                all_needing_rendering = self.db.get_papers_needing_rendering()
                count = len(all_needing_rendering)
                print(f"\n  -> Next Step: RENDER to Markdown ({count} XMLs need rendering)")
                self.step_4_render_markdown()

            elif extracted < converted:
                # Some XMLs still need markdown rendering
                print(f"\n  -> Resuming from STEP 4 ({converted - extracted} XMLs need rendering)")
                self.step_4_render_markdown()

            else:
                print("\n  [OK] Pipeline appears complete - nothing to resume.")
                print("    Use --reset status to re-run specific stages.")
                return

            self._print_final_summary()

        except KeyboardInterrupt:
            print("\n\n  Pipeline interrupted by user.")
        except Exception as e:
            print(f"\n\n  Pipeline error: {e}")
            raise

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


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python src/main.py",
        description="IDRD Pipeline — Research Paper Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
MODES
-----
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
                      help="Run markdown rendering on existing XMLs in data/xml/")
    mode.add_argument("--resume",        action="store_true",
                      help="Check database state and run the next incomplete step (download/convert/render)")
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
                    help="Skip markdown rendering step in full pipeline")

    return parser


def main():
    parser   = build_parser()
    args     = parser.parse_args()
    pipeline = IDRDPipeline()

    try:
        if args.status:
            pipeline.print_status()

        elif args.reset:
            pipeline.reset_pipeline(args.reset)

        elif args.resume:
            pipeline.resume_pipeline()

        elif args.fetch_only:
            if not args.query:
                parser.error("--query is required for --fetch-only")
            pipeline.step_1_fetch_papers(
                query=args.query,
                limit=args.limit,
                open_access_only=not args.all_access,
                fields_of_study=args.fields_of_study,
            )

        elif args.download_only:
            pipeline.step_2_download_pdfs(
                limit=args.dl_limit,
                overwrite=args.dl_overwrite,
                delay=args.dl_delay,
            )

        elif args.convert_only:
            pipeline.step_3_convert_to_xml(
                limit=args.cv_limit,
                overwrite=args.cv_overwrite,
                delete_pdf=args.delete_pdfs,
                delay=args.cv_delay,
            )

        elif args.extract_only:
            pipeline.step_4_render_markdown(
                limit=args.ex_limit,
                overwrite=args.ex_overwrite,
            )

        else:
            # Full pipeline — query is required here only
            if not args.query:
                parser.error("--query is required to run the full pipeline")
            pipeline.run_full_pipeline(
                query=args.query,
                limit=args.limit,
                open_access_only=not args.all_access,
                convert_to_xml=not args.no_xml,
                render_markdown=not args.no_extract,
                delete_pdfs_after_conversion=args.delete_pdfs,
            )

    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()