"""
Shared database helper utilities.
Centralises repeated query patterns used across downloader, converter, and main.

Includes new persistence functions for results-based API.
"""
from pathlib import Path
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from db.db import IDRDDatabase
    from models.results import DownloadResult, ConversionResult, RenderResult


def print_download_status(db: "IDRDDatabase", output_dir: Path):
    """Print current download status from the publications table."""
    db.cursor.execute("""
        SELECT
            COUNT(*)                                           AS total,
            COUNT(*) FILTER (WHERE pdf_downloaded = TRUE)     AS downloaded,
            COUNT(*) FILTER (
                WHERE pdf_download_error IS NOT NULL
                  AND pdf_download_error != ''
            )                                                  AS errors
        FROM publications
    """)
    row = db.cursor.fetchone()

    pdf_files = list(output_dir.glob("*.pdf")) if output_dir.exists() else []

    print("\nCurrent Download Status:")
    print("-" * 60)
    print(f"  Total papers in DB  : {row['total']}")
    print(f"  PDFs downloaded     : {row['downloaded']}")
    print(f"  Download errors     : {row['errors']}")
    print(f"  PDF files on disk   : {len(pdf_files)}")
    print("-" * 60)


def print_conversion_status(db: "IDRDDatabase", xml_output_dir: Path):
    """Print current XML conversion status."""
    print("\nCurrent Conversion Status:")
    print("-" * 60)

    status = db.get_pipeline_status()
    print(f"  Papers with PDFs    : {status['pdf_downloaded']}")
    print(f"  Converted to XML    : {status['xml_converted']}")
    print(f"  Conversion errors   : {status['xml_errors']}")
    print(f"  XML files on disk   : {len(list(xml_output_dir.glob('*.tei.xml')))}")
    print("-" * 60)


def sync_existing_pdfs(db: "IDRDDatabase", pdf_output_dir: Path) -> int:
    """
    Sync PDF files on disk with the database.
    Marks papers as downloaded if the PDF exists but the DB flag is not set.

    Returns number of records synced.
    """
    existing_pdfs = list(pdf_output_dir.glob("*.pdf"))
    if not existing_pdfs:
        return 0

    status = db.get_pipeline_status()
    if status['pdf_downloaded'] >= len(existing_pdfs):
        return 0  # already in sync

    print(f"\nSyncing {len(existing_pdfs)} existing PDFs with database...")
    synced = 0

    for pdf_file in existing_pdfs:
        paper_id = pdf_file.stem

        db.cursor.execute(
            'SELECT "paperId", pdf_downloaded FROM publications WHERE "paperId" = %s',
            (paper_id,)
        )
        result = db.cursor.fetchone()

        if result and not result['pdf_downloaded']:
            db.cursor.execute('''
                UPDATE publications SET
                    pdf_downloaded     = TRUE,
                    pdf_download_date  = CURRENT_TIMESTAMP,
                    pdf_path           = %s,
                    pdf_download_error = NULL,
                    updated_at         = CURRENT_TIMESTAMP
                WHERE "paperId" = %s
            ''', (str(pdf_file), paper_id))
            synced += 1

    db.commit()
    print(f"  Synced {synced} PDFs")
    return synced


def update_pdf_status(db: "IDRDDatabase", paper_id: str, success: bool,
                      pdf_path: str = None, error: str = None):
    """Update PDF download status for a single paper."""
    if success:
        db.cursor.execute('''
            UPDATE publications SET
                pdf_downloaded     = TRUE,
                pdf_download_date  = CURRENT_TIMESTAMP,
                pdf_path           = %s,
                pdf_download_error = NULL,
                updated_at         = CURRENT_TIMESTAMP
            WHERE "paperId" = %s
        ''', (pdf_path, paper_id))
    else:
        db.cursor.execute('''
            UPDATE publications SET
                pdf_downloaded     = FALSE,
                pdf_download_error = %s,
                updated_at         = CURRENT_TIMESTAMP
            WHERE "paperId" = %s
        ''', (error, paper_id))
    db.commit()


def update_xml_status(db: "IDRDDatabase", paper_id: str, success: bool,
                      xml_path: str = None, error: str = None):
    """Update XML conversion status for a single paper."""
    if success:
        db.cursor.execute('''
            UPDATE publications SET
                xml_converted        = TRUE,
                xml_conversion_date  = CURRENT_TIMESTAMP,
                xml_path             = %s,
                xml_conversion_error = NULL,
                updated_at           = CURRENT_TIMESTAMP
            WHERE "paperId" = %s
        ''', (xml_path, paper_id))
    else:
        db.cursor.execute('''
            UPDATE publications SET
                xml_converted        = FALSE,
                xml_conversion_error = %s,
                updated_at           = CURRENT_TIMESTAMP
            WHERE "paperId" = %s
        ''', (error, paper_id))
    db.commit()


# ──────────────────────────────────────────────────────────────────────────────
# NEW API: Batch persistence functions for results-based components
# ──────────────────────────────────────────────────────────────────────────────

def persist_download_results(db: "IDRDDatabase", results: List["DownloadResult"]) -> int:
    """
    Persist download results to database in batch.
    
    Args:
        db: Database instance
        results: List of DownloadResult objects
        
    Returns:
        Number of records updated
    """
    updated = 0
    for result in results:
        pdf_path = str(result.filepath) if result.filepath else None
        update_pdf_status(db, result.paper_id, result.success, 
                         pdf_path=pdf_path, error=result.error)
        updated += 1
    return updated


def persist_conversion_results(db: "IDRDDatabase", results: List["ConversionResult"]) -> int:
    """
    Persist conversion results to database in batch.
    
    Args:
        db: Database instance
        results: List of ConversionResult objects
        
    Returns:
        Number of records updated
    """
    updated = 0
    for result in results:
        xml_path = str(result.xml_path) if result.xml_path else None
        update_xml_status(db, result.paper_id, result.success,
                         xml_path=xml_path, error=result.error)
        updated += 1
    return updated


def persist_render_results(db: "IDRDDatabase", results: List["RenderResult"]) -> int:
    """
    Persist render results to database in batch.
    
    This updates markdown_extracted flag and markdown_path in the database.
    
    Args:
        db: Database instance
        results: List of RenderResult objects
        
    Returns:
        Number of records updated
    """
    updated = 0
    for result in results:
        md_path = str(result.md_path) if result.md_path else None
        
        if result.success:
            db.cursor.execute('''
                UPDATE publications SET
                    markdown_extracted    = TRUE,
                    markdown_extract_date = CURRENT_TIMESTAMP,
                    markdown_path         = %s,
                    markdown_error        = NULL,
                    updated_at            = CURRENT_TIMESTAMP
                WHERE "paperId" = %s
            ''', (md_path, result.paper_id))
        else:
            db.cursor.execute('''
                UPDATE publications SET
                    markdown_extracted = FALSE,
                    markdown_error     = %s,
                    updated_at         = CURRENT_TIMESTAMP
                WHERE "paperId" = %s
            ''', (result.error, result.paper_id))
        updated += 1
    
    db.commit()
    return updated