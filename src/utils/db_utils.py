"""
Shared database helper utilities.
Centralises repeated query patterns used across downloader, converter, and main.
"""
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.db import PublicationDatabase


def print_download_status(db: "PublicationDatabase", output_dir: Path):
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


def print_conversion_status(db: "PublicationDatabase", xml_output_dir: Path):
    """Print current XML conversion status."""
    print("\nCurrent Conversion Status:")
    print("-" * 60)

    status = db.get_pipeline_status()
    print(f"  Papers with PDFs    : {status['pdf_downloaded']}")
    print(f"  Converted to XML    : {status['xml_converted']}")
    print(f"  Conversion errors   : {status['xml_errors']}")
    print(f"  XML files on disk   : {len(list(xml_output_dir.glob('*.tei.xml')))}")
    print("-" * 60)


def sync_existing_pdfs(db: "PublicationDatabase", pdf_output_dir: Path) -> int:
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


def update_pdf_status(db: "PublicationDatabase", paper_id: str, success: bool,
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


def update_xml_status(db: "PublicationDatabase", paper_id: str, success: bool,
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