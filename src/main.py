"""
IDRD Pipeline - Main Orchestrator
==================================
Orchestrates the complete paper retrieval, download, and conversion pipeline.

Pipeline Flow:
1. Fetch papers from Semantic Scholar
2. Save to database
3. Download PDFs
4. Convert PDFs to TEI XML
5. Extract sections (TODO)
6. Extract features with LLM (TODO)
"""

import sys
from pathlib import Path
import json
import argparse
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from pubfetcher.fetching import SemanticScholarClient
from utils.dict_parser import PaperDictParser
from db.db import PublicationDatabase
from extractor.downloader import PDFDownloader
from extractor.converter import GrobidConverter


class IDRDPipeline:
    """Main pipeline orchestrator for IDRD paper processing."""
    
    def __init__(self, db_path: str = None):
        """
        Initialize pipeline components.
        
        Args:
            db_path: Path to database file (default: src/db/publications.db)
        """
        self.db = PublicationDatabase(db_path)
        self.parser = PaperDictParser()
        self.start_time = datetime.now()
        
        print("\n" + "="*70)
        print("IDRD PIPELINE INITIALIZED")
        print("="*70)
        print(f"Database: {self.db.db_path}")
        print(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
    
    def step_1_fetch_papers(
        self,
        query: str,
        limit: int = 100,
        open_access_only: bool = True,
        fields_of_study: str = None,
        fetch_citations: bool = True,
        max_citations_per_paper: int = 50
    ) -> int:
        """
        Step 1: Fetch papers from Semantic Scholar API.
        
        Args:
            query: Search query string
            limit: Maximum number of papers to fetch
            open_access_only: If True, only fetch papers with PDF URLs
            fields_of_study: Filter by fields of study (e.g., "Computer Science")
            fetch_citations: If True, fetch citation contexts and intents
            max_citations_per_paper: Maximum citations to fetch per paper
            
        Returns:
            Number of papers fetched
        """
        print("\n" + "="*70)
        print("STEP 1: FETCHING PAPERS FROM SEMANTIC SCHOLAR")
        print("="*70)
        print(f"Query: '{query}'")
        print(f"Limit: {limit}")
        print(f"Open access only: {open_access_only}")
        if fields_of_study:
            print(f"Fields of study: {fields_of_study}")
        print("="*70 + "\n")
        
        # Initialize client and fetch papers
        client = SemanticScholarClient()
        papers = client.search_papers(
            query=query,
            limit=limit,
            open_access_pdf=open_access_only,
            fields_of_study=fields_of_study
        )
        
        if not papers:
            print("\nNo papers found matching criteria")
            return 0
        
        # Optionally fetch citation contexts
        if fetch_citations:
            print("\n" + "-"*70)
            papers = client.enrich_papers_with_citations(
                papers,
                max_citations_per_paper=max_citations_per_paper
            )
        
        # Parse papers
        self.parser.parse_papers(papers)
        
        # Save to JSON (backup)
        json_path = Path(__file__).parent.parent / 'outputs' / 'metadata' / 'retrieved_results.json'
        json_path.parent.mkdir(parents=True, exist_ok=True)
        self.parser.to_json(str(json_path))
        
        # Save to database
        print("\nSaving to database...")
        count = self.db.insert_publications(papers)
        
        # Print summary
        open_access_count = sum(1 for p in papers if p.get('openAccessPdf', {}).get('url'))
        papers_with_doi = sum(1 for p in papers if p.get('externalIds', {}).get('DOI'))
        papers_with_citations = sum(1 for p in papers if p.get('citations'))
        
        print("\n" + "-"*70)
        print("STEP 1 SUMMARY")
        print("-"*70)
        print(f"Total papers fetched: {len(papers)}")
        print(f"Papers saved to database: {count}")
        print(f"Papers with PDF URLs: {open_access_count}")
        print(f"Papers with DOI: {papers_with_doi}")
        if fetch_citations:
            print(f"Papers with citation data: {papers_with_citations}")
        print(f"JSON backup saved to: {json_path}")
        print("-"*70)
        
        return count
    
    def step_2_download_pdfs(
        self,
        limit: int = None,
        overwrite: bool = False,
        delay: float = 0.5
    ) -> dict:
        """
        Step 2: Download PDFs for papers in database.
        
        Args:
            limit: Maximum number of PDFs to download (None = all)
            overwrite: If True, re-download existing PDFs
            delay: Delay between downloads in seconds
            
        Returns:
            Dictionary with download results
        """
        print("\n" + "="*70)
        print("STEP 2: DOWNLOADING PDFs")
        print("="*70)
        print(f"Limit: {limit if limit else 'All available'}")
        print(f"Overwrite existing: {overwrite}")
        print(f"Delay between downloads: {delay}s")
        print("="*70 + "\n")
        
        # Initialize downloader
        downloader = PDFDownloader(db_path=str(self.db.db_path))
        
        try:
            # Check database status
            self._print_download_status(downloader)
            
            # Sync existing PDFs with database
            self._sync_existing_pdfs(downloader)
            
            # Download remaining PDFs
            print("\n" + "-"*70)
            print("DOWNLOADING PDFS")
            print("-"*70)
            
            results = downloader.download_from_database(
                limit=limit,
                overwrite=overwrite,
                delay=delay
            )
            
            # Print statistics
            downloader.print_statistics()
            
            # Print final status
            print("\n" + "-"*70)
            print("STEP 2 COMPLETE")
            print("-"*70)
            self._print_download_status(downloader)
            
            # Save results
            results_path = Path(__file__).parent.parent / 'outputs' / 'metadata' / 'download_results.json'
            results_path.parent.mkdir(parents=True, exist_ok=True)
            with open(results_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Results saved to: {results_path}")
            print("-"*70)
            
            return results
            
        finally:
            downloader.close()
    
    def step_3_convert_to_xml(
        self,
        limit: int = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
        delay: float = 0.1
    ) -> dict:
        """
        Step 3: Convert PDFs to TEI XML using GROBID.
        
        Args:
            limit: Maximum number of PDFs to convert (None = all)
            overwrite: If True, re-convert existing XMLs
            delete_pdf: If True, delete PDFs after successful conversion
            delay: Delay between conversions in seconds
            
        Returns:
            Dictionary with conversion results
        """
        print("\n" + "="*70)
        print("STEP 3: CONVERTING PDFs TO TEI XML")
        print("="*70)
        print(f"Limit: {limit if limit else 'All available'}")
        print(f"Overwrite existing: {overwrite}")
        print(f"Delete PDFs after conversion: {delete_pdf}")
        print(f"Delay between conversions: {delay}s")
        print("="*70 + "\n")
        
        # Initialize converter
        converter = GrobidConverter(db_path=str(self.db.db_path))
        
        try:
            # Check database status
            self._print_conversion_status(converter)
            
            # Start GROBID container
            print("\n" + "-"*70)
            print("STARTING GROBID DOCKER CONTAINER")
            print("-"*70)
            converter.start_grobid(wait_time=30)
            
            # Convert PDFs
            print("\n" + "-"*70)
            print("CONVERTING PDFs TO XML")
            print("-"*70)
            
            results = converter.convert_from_database(
                limit=limit,
                overwrite=overwrite,
                delete_pdf=delete_pdf,
                delay=delay
            )
            
            # Print statistics
            converter.print_statistics()
            
            # Print final status
            print("\n" + "-"*70)
            print("STEP 3 COMPLETE")
            print("-"*70)
            self._print_conversion_status(converter)
            
            # Save results
            results_path = Path(__file__).parent.parent / 'outputs' / 'metadata' / 'conversion_results.json'
            results_path.parent.mkdir(parents=True, exist_ok=True)
            with open(results_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Results saved to: {results_path}")
            print("-"*70)
            
            # Stop GROBID
            print("\nStopping GROBID container...")
            converter.stop_grobid()
            
            return results
            
        except Exception as e:
            print(f"\nError in conversion step: {e}")
            converter.stop_grobid()
            raise
        finally:
            converter.close_db()
    
    def step_4_extract_sections(self):
        """Step 4: Extract sections from TEI XML files (TODO)."""
        print("\n" + "="*70)
        print("STEP 4: EXTRACTING SECTIONS FROM XML")
        print("="*70)
        print("TODO: Implement section extraction")
        print("="*70)
    
    def step_5_extract_features(self):
        """Step 5: Extract features using LLM (TODO)."""
        print("\n" + "="*70)
        print("STEP 5: EXTRACTING FEATURES WITH LLM")
        print("="*70)
        print("TODO: Implement LLM feature extraction")
        print("="*70)
    
    def run_full_pipeline(
        self,
        query: str,
        limit: int = 100,
        open_access_only: bool = True,
        fetch_citations: bool = True,
        convert_to_xml: bool = True,
        delete_pdfs_after_conversion: bool = False
    ):
        """
        Run the complete pipeline from start to finish.
        
        Args:
            query: Search query for papers
            limit: Maximum number of papers to process
            open_access_only: Only fetch papers with PDFs
            fetch_citations: Fetch citation contexts
            convert_to_xml: Convert PDFs to XML
            delete_pdfs_after_conversion: Delete PDFs after XML conversion
        """
        print("\n" + "="*70)
        print("RUNNING FULL IDRD PIPELINE")
        print("="*70)
        print(f"Query: '{query}'")
        print(f"Paper limit: {limit}")
        print("="*70)
        
        try:
            # Step 1: Fetch papers
            papers_fetched = self.step_1_fetch_papers(
                query=query,
                limit=limit,
                open_access_only=open_access_only,
                fetch_citations=fetch_citations
            )
            
            if papers_fetched == 0:
                print("\nNo papers fetched. Pipeline stopped.")
                return
            
            # Step 2: Download PDFs
            download_results = self.step_2_download_pdfs()
            
            if download_results['stats']['successful'] == 0:
                print("\nNo PDFs downloaded. Skipping conversion step.")
            elif convert_to_xml:
                # Step 3: Convert to XML
                self.step_3_convert_to_xml(
                    delete_pdf=delete_pdfs_after_conversion
                )
            
            # Future steps
            # self.step_4_extract_sections()
            # self.step_5_extract_features()
            
            # Final summary
            self._print_final_summary()
            
        except KeyboardInterrupt:
            print("\n\nPipeline interrupted by user")
        except Exception as e:
            print(f"\n\nPipeline error: {e}")
            raise
        finally:
            self.cleanup()
    
    def _print_download_status(self, downloader: PDFDownloader):
        """Print current download status."""
        print("\nCurrent Database Status:")
        print("-"*70)
        
        downloader.db.cursor.execute('SELECT COUNT(*) FROM publications')
        total = downloader.db.cursor.fetchone()[0]
        print(f"Total papers in database: {total}")
        
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications p
            JOIN open_access oa ON p.paperId = oa.paperId
            WHERE oa.url IS NOT NULL
        ''')
        with_urls = downloader.db.cursor.fetchone()[0]
        print(f"Papers with PDF URLs: {with_urls}")
        
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications WHERE pdf_downloaded = 1
        ''')
        downloaded = downloader.db.cursor.fetchone()[0]
        print(f"Papers downloaded: {downloaded}")
        
        existing_pdfs = list(downloader.output_dir.glob("*.pdf"))
        print(f"PDF files on disk: {len(existing_pdfs)}")
        
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications 
            WHERE pdf_download_error IS NOT NULL
        ''')
        errors = downloader.db.cursor.fetchone()[0]
        print(f"Download errors: {errors}")
        print("-"*70)
    
    def _print_conversion_status(self, converter: GrobidConverter):
        """Print current conversion status."""
        print("\nCurrent Database Status:")
        print("-"*70)
        
        converter.db.cursor.execute('''
            SELECT COUNT(*) FROM publications WHERE pdf_downloaded = 1
        ''')
        with_pdfs = converter.db.cursor.fetchone()[0]
        print(f"Papers with PDFs: {with_pdfs}")
        
        converter.db.cursor.execute('''
            SELECT COUNT(*) FROM publications WHERE xml_converted = 1
        ''')
        converted = converter.db.cursor.fetchone()[0]
        print(f"Papers converted to XML: {converted}")
        
        existing_xmls = list(converter.output_dir.glob("*.tei.xml"))
        print(f"XML files on disk: {len(existing_xmls)}")
        
        converter.db.cursor.execute('''
            SELECT COUNT(*) FROM publications 
            WHERE xml_conversion_error IS NOT NULL
        ''')
        errors = converter.db.cursor.fetchone()[0]
        print(f"Conversion errors: {errors}")
        print("-"*70)
    
    def _sync_existing_pdfs(self, downloader: PDFDownloader):
        """Sync existing PDF files with database."""
        existing_pdfs = list(downloader.output_dir.glob("*.pdf"))
        
        downloader.db.cursor.execute('''
            SELECT COUNT(*) FROM publications WHERE pdf_downloaded = 1
        ''')
        already_downloaded = downloader.db.cursor.fetchone()[0]
        
        if len(existing_pdfs) > 0 and already_downloaded < len(existing_pdfs):
            print(f"\nSyncing {len(existing_pdfs)} existing PDFs with database...")
            synced = 0
            
            for pdf_file in existing_pdfs:
                paper_id = pdf_file.stem
                pdf_path = str(pdf_file)
                
                downloader.db.cursor.execute(
                    'SELECT paperId, pdf_downloaded FROM publications WHERE paperId = ?',
                    (paper_id,)
                )
                result = downloader.db.cursor.fetchone()
                
                if result and result[1] != 1:
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
            
            downloader.db.conn.commit()
            print(f"Synced {synced} PDFs with database")
    
    def _print_final_summary(self):
        """Print final pipeline summary."""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print("\n" + "="*70)
        print("PIPELINE EXECUTION COMPLETE")
        print("="*70)
        
        stats = self.db.get_statistics()
        pipeline_status = self.get_pipeline_status()
        
        print("\nDatabase Summary:")
        print(f"  Total papers: {stats['total_publications']}")
        print(f"  Papers with PDFs downloaded: {pipeline_status['pdf_downloaded']}")
        print(f"  Papers converted to XML: {pipeline_status['xml_converted']}")
        print(f"  Papers with download errors: {pipeline_status['pdf_errors']}")
        print(f"  Papers with conversion errors: {pipeline_status['xml_errors']}")
        
        print(f"\nExecution Time:")
        print(f"  Start: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  End: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Duration: {duration}")
        
        print("="*70)
    
    def get_pipeline_status(self) -> dict:
        """Get current pipeline processing status."""
        status = {}
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications')
        status['total_papers'] = self.db.cursor.fetchone()[0]
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications WHERE pdf_downloaded = 1')
        status['pdf_downloaded'] = self.db.cursor.fetchone()[0]
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications WHERE xml_converted = 1')
        status['xml_converted'] = self.db.cursor.fetchone()[0]
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications WHERE sections_extracted = 1')
        status['sections_extracted'] = self.db.cursor.fetchone()[0]
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications WHERE features_extracted = 1')
        status['features_extracted'] = self.db.cursor.fetchone()[0]
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications WHERE pdf_download_error IS NOT NULL')
        status['pdf_errors'] = self.db.cursor.fetchone()[0]
        
        self.db.cursor.execute('SELECT COUNT(*) FROM publications WHERE xml_conversion_error IS NOT NULL')
        status['xml_errors'] = self.db.cursor.fetchone()[0]
        
        return status
    
    def reset_pipeline(self, reset_type: str = "status"):
        """
        Reset pipeline state.
        
        Args:
            reset_type: "status" (reset pipeline tracking) or "full" (delete all data)
        """
        if reset_type == "full":
            print("\n" + "="*70)
            print("WARNING: FULL DATABASE RESET")
            print("="*70)
            print("This will PERMANENTLY DELETE ALL data in the database:")
            print("  - All publications and metadata")
            print("  - All authors and citations")
            print("  - All pipeline tracking data")
            print("  - Everything will be wiped clean")
            print("="*70)
            
            # First confirmation
            confirm1 = input("\nType 'yes' to continue: ")
            if confirm1.lower() != 'yes':
                print("Reset cancelled")
                return
            
            # Second confirmation with full name
            print("\n" + "="*70)
            print("FINAL CONFIRMATION REQUIRED")
            print("="*70)
            confirm2 = input("Type your full name 'Omer Nidam' to confirm: ")
            if confirm2 == "Omer Nidam":
                self.db.reset_database(confirm=True)
                print("\n" + "="*70)
                print("DATABASE FULLY RESET")
                print("="*70)
            else:
                print("\nIncorrect name. Reset cancelled for safety.")
                print("Database remains unchanged.")
        elif reset_type == "status":
            self.db.reset_pipeline_status()
        else:
            print(f"Unknown reset type: {reset_type}")
    
    def cleanup(self):
        """Cleanup resources."""
        self.db.close()
        print("\nPipeline resources cleaned up")


# CLI interface
def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="IDRD Pipeline - Research Paper Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python src/main.py --query "Transformers" --limit 50
  
  # Fetch papers only
  python src/main.py --query "Machine Learning" --limit 100 --fetch-only
  
  # Download PDFs only
  python src/main.py --download-only
  
  # Convert PDFs to XML only
  python src/main.py --convert-only
  
  # Reset pipeline status
  python src/main.py --reset status
        """
    )
    
    # Pipeline control
    parser.add_argument('--query', type=str, help='Search query for papers')
    parser.add_argument('--limit', type=int, default=100, help='Maximum papers to process')
    parser.add_argument('--fetch-only', action='store_true', help='Only fetch papers, do not download')
    parser.add_argument('--download-only', action='store_true', help='Only download PDFs')
    parser.add_argument('--convert-only', action='store_true', help='Only convert PDFs to XML')
    parser.add_argument('--no-xml', action='store_true', help='Skip XML conversion')
    parser.add_argument('--delete-pdfs', action='store_true', help='Delete PDFs after conversion')
    parser.add_argument('--reset', type=str, choices=['status', 'full'], help='Reset pipeline')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    
    # Options
    parser.add_argument('--no-citations', action='store_true', help='Skip fetching citations')
    parser.add_argument('--fields-of-study', type=str, help='Filter by fields of study')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = IDRDPipeline()
    
    try:
        # Reset
        if args.reset:
            pipeline.reset_pipeline(args.reset)
            return
        
        # Status
        if args.status:
            status = pipeline.get_pipeline_status()
            print("\nPipeline Status:")
            print(json.dumps(status, indent=2))
            return
        
        # Individual steps
        if args.fetch_only:
            if not args.query:
                print("Error: --query required for fetching")
                return
            pipeline.step_1_fetch_papers(
                query=args.query,
                limit=args.limit,
                fetch_citations=not args.no_citations,
                fields_of_study=args.fields_of_study
            )
        elif args.download_only:
            pipeline.step_2_download_pdfs()
        elif args.convert_only:
            pipeline.step_3_convert_to_xml(delete_pdf=args.delete_pdfs)
        else:
            # Full pipeline
            if not args.query:
                print("Error: --query required for full pipeline")
                return
            pipeline.run_full_pipeline(
                query=args.query,
                limit=args.limit,
                fetch_citations=not args.no_citations,
                convert_to_xml=not args.no_xml,
                delete_pdfs_after_conversion=args.delete_pdfs
            )
    
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()