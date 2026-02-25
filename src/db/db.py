import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
import os
from dotenv import load_dotenv

load_dotenv()


class PublicationDatabase:
    """PostgreSQL database manager for storing publication metadata."""

    def __init__(self, connection_string: str = None):
        """
        Initialize database connection.

        Args:
            connection_string: PostgreSQL DSN. Falls back to env vars.
        """
        if connection_string is None:
            connection_string = (
                f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
                f"port={os.getenv('POSTGRES_PORT', '5432')} "
                f"dbname={os.getenv('POSTGRES_DB', 'idrd_pipeline')} "
                f"user={os.getenv('POSTGRES_USER', 'postgres')} "
                f"password={os.getenv('POSTGRES_PASSWORD', '')}"
            )

        self.connection_string = connection_string
        self.conn = psycopg2.connect(connection_string)
        # RealDictCursor returns rows as dicts (same behaviour as sqlite3.Row)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        self._create_tables()
        print("✓ PostgreSQL database initialized")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_tables(self):
        """Create database tables if they don't exist."""

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publications (
                "paperId"                    TEXT PRIMARY KEY,
                title                        TEXT NOT NULL,
                abstract                     TEXT,
                year                         INTEGER,
                url                          TEXT,
                venue                        TEXT,
                "publicationDate"            TEXT,
                "citationCount"              INTEGER DEFAULT 0,
                "referenceCount"             INTEGER DEFAULT 0,
                "influentialCitationCount"   INTEGER DEFAULT 0,
                tldr                         TEXT,
                "isOpenAccess"               BOOLEAN,

                -- Pipeline tracking
                pdf_downloaded               BOOLEAN DEFAULT FALSE,
                pdf_download_date            TIMESTAMP,
                pdf_path                     TEXT,
                pdf_download_error           TEXT,

                xml_converted                BOOLEAN DEFAULT FALSE,
                xml_conversion_date          TIMESTAMP,
                xml_path                     TEXT,
                xml_conversion_error         TEXT,

                sections_extracted           BOOLEAN DEFAULT FALSE,
                sections_extraction_date     TIMESTAMP,
                sections_extraction_error    TEXT,

                features_extracted           BOOLEAN DEFAULT FALSE,
                features_extraction_date     TIMESTAMP,
                features_extraction_error    TEXT,

                created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS authors (
                "authorId"  TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                url         TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publication_authors (
                "paperId"   TEXT REFERENCES publications("paperId") ON DELETE CASCADE,
                "authorId"  TEXT REFERENCES authors("authorId")     ON DELETE CASCADE,
                PRIMARY KEY ("paperId", "authorId")
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS external_ids (
                "paperId"  TEXT PRIMARY KEY REFERENCES publications("paperId") ON DELETE CASCADE,
                doi        TEXT,
                arxiv      TEXT,
                pubmed     TEXT,
                dblp       TEXT,
                "corpusId" TEXT
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS open_access (
                "paperId"   TEXT PRIMARY KEY REFERENCES publications("paperId") ON DELETE CASCADE,
                url         TEXT,
                status      TEXT,
                license     TEXT,
                disclaimer  TEXT
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS journals (
                "paperId"  TEXT PRIMARY KEY REFERENCES publications("paperId") ON DELETE CASCADE,
                name       TEXT,
                volume     TEXT,
                pages      TEXT
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publication_types (
                "paperId"  TEXT REFERENCES publications("paperId") ON DELETE CASCADE,
                type       TEXT,
                PRIMARY KEY ("paperId", type)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fields_of_study (
                "paperId"  TEXT REFERENCES publications("paperId") ON DELETE CASCADE,
                field      TEXT,
                PRIMARY KEY ("paperId", field)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citations (
                id                   SERIAL PRIMARY KEY,
                "paperId"            TEXT NOT NULL REFERENCES publications("paperId") ON DELETE CASCADE,
                "citingPaperId"      TEXT,
                "citingPaperTitle"   TEXT,
                "citingPaperYear"    INTEGER,
                "isInfluential"      BOOLEAN DEFAULT FALSE,
                created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE("paperId", "citingPaperId")
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citation_contexts (
                id          SERIAL PRIMARY KEY,
                citation_id INTEGER NOT NULL REFERENCES citations(id) ON DELETE CASCADE,
                context     TEXT NOT NULL
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citation_intents (
                id          SERIAL PRIMARY KEY,
                citation_id INTEGER NOT NULL REFERENCES citations(id) ON DELETE CASCADE,
                intent      TEXT NOT NULL
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citation_authors (
                citation_id INTEGER NOT NULL REFERENCES citations(id) ON DELETE CASCADE,
                "authorId"  TEXT,
                name        TEXT NOT NULL,
                PRIMARY KEY (citation_id, "authorId")
            )
        ''')

        # --- indexes ---
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_year              ON publications(year)',
            'CREATE INDEX IF NOT EXISTS idx_citations         ON publications("citationCount")',
            'CREATE INDEX IF NOT EXISTS idx_title             ON publications(title)',
            'CREATE INDEX IF NOT EXISTS idx_doi               ON external_ids(doi)',
            'CREATE INDEX IF NOT EXISTS idx_citing_paper      ON citations("citingPaperId")',
            'CREATE INDEX IF NOT EXISTS idx_citation_paper    ON citations("paperId")',
            'CREATE INDEX IF NOT EXISTS idx_pdf_downloaded    ON publications(pdf_downloaded)',
            'CREATE INDEX IF NOT EXISTS idx_xml_converted     ON publications(xml_converted)',
            'CREATE INDEX IF NOT EXISTS idx_sections_extracted ON publications(sections_extracted)',
            'CREATE INDEX IF NOT EXISTS idx_features_extracted ON publications(features_extracted)',
        ]
        for idx in indexes:
            self.cursor.execute(idx)

        self.conn.commit()

    # ------------------------------------------------------------------
    # Inserts
    # ------------------------------------------------------------------

    def insert_publication(self, paper: Dict[str, Any]) -> bool:
        """Insert a single publication into the database."""
        try:
            tldr = paper.get('tldr')
            if isinstance(tldr, dict):
                tldr = tldr.get('text')

            open_access_pdf = paper.get('openAccessPdf')
            is_open_access = False
            if isinstance(open_access_pdf, dict):
                is_open_access = bool(open_access_pdf.get('url'))
            elif open_access_pdf:
                is_open_access = True

            # publications — INSERT ... ON CONFLICT replaces sqlite's INSERT OR REPLACE
            self.cursor.execute('''
                INSERT INTO publications (
                    "paperId", title, abstract, year, url, venue, "publicationDate",
                    "citationCount", "referenceCount", "influentialCitationCount",
                    tldr, "isOpenAccess"
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT ("paperId") DO UPDATE SET
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    year = EXCLUDED.year,
                    url = EXCLUDED.url,
                    venue = EXCLUDED.venue,
                    "publicationDate" = EXCLUDED."publicationDate",
                    "citationCount" = EXCLUDED."citationCount",
                    "referenceCount" = EXCLUDED."referenceCount",
                    "influentialCitationCount" = EXCLUDED."influentialCitationCount",
                    tldr = EXCLUDED.tldr,
                    "isOpenAccess" = EXCLUDED."isOpenAccess",
                    updated_at = CURRENT_TIMESTAMP
            ''', (
                paper.get('paperId'),
                paper.get('title'),
                paper.get('abstract'),
                paper.get('year'),
                paper.get('url'),
                paper.get('venue'),
                paper.get('publicationDate'),
                paper.get('citationCount', 0),
                paper.get('referenceCount', 0),
                paper.get('influentialCitationCount', 0),
                tldr,
                is_open_access,
            ))

            paper_id = paper.get('paperId')

            # authors
            for author in paper.get('authors', []):
                author_id = author.get('authorId')
                if author_id:
                    self.cursor.execute('''
                        INSERT INTO authors ("authorId", name, url)
                        VALUES (%s, %s, %s)
                        ON CONFLICT ("authorId") DO NOTHING
                    ''', (author_id, author.get('name'), author.get('url')))

                    self.cursor.execute('''
                        INSERT INTO publication_authors ("paperId", "authorId")
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    ''', (paper_id, author_id))

            # external ids
            ext_ids = paper.get('externalIds', {})
            if ext_ids:
                self.cursor.execute('''
                    INSERT INTO external_ids ("paperId", doi, arxiv, pubmed, dblp, "corpusId")
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT ("paperId") DO UPDATE SET
                        doi      = EXCLUDED.doi,
                        arxiv    = EXCLUDED.arxiv,
                        pubmed   = EXCLUDED.pubmed,
                        dblp     = EXCLUDED.dblp,
                        "corpusId" = EXCLUDED."corpusId"
                ''', (
                    paper_id,
                    ext_ids.get('DOI'),
                    ext_ids.get('ArXiv'),
                    ext_ids.get('PubMed'),
                    ext_ids.get('DBLP'),
                    ext_ids.get('CorpusId'),
                ))

            # open access
            oa = paper.get('openAccessPdf')
            if oa:
                self.cursor.execute('''
                    INSERT INTO open_access ("paperId", url, status, license, disclaimer)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT ("paperId") DO UPDATE SET
                        url = EXCLUDED.url, status = EXCLUDED.status,
                        license = EXCLUDED.license, disclaimer = EXCLUDED.disclaimer
                ''', (
                    paper_id,
                    oa.get('url'), oa.get('status'), oa.get('license'), oa.get('disclaimer'),
                ))

            # journal
            journal = paper.get('journal')
            if journal:
                self.cursor.execute('''
                    INSERT INTO journals ("paperId", name, volume, pages)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT ("paperId") DO UPDATE SET
                        name = EXCLUDED.name, volume = EXCLUDED.volume, pages = EXCLUDED.pages
                ''', (paper_id, journal.get('name'), journal.get('volume'), journal.get('pages')))

            # publication types
            for pub_type in paper.get('publicationTypes', []):
                self.cursor.execute('''
                    INSERT INTO publication_types ("paperId", type)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                ''', (paper_id, pub_type))

            # fields of study
            for field in paper.get('fieldsOfStudy', []):
                self.cursor.execute('''
                    INSERT INTO fields_of_study ("paperId", field)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                ''', (paper_id, field))

            # citations
            for citation in paper.get('citations', []):
                citing_paper = citation.get('citingPaper', {})
                citing_paper_id = citing_paper.get('paperId') if citing_paper else None

                self.cursor.execute('''
                    INSERT INTO citations
                        ("paperId","citingPaperId","citingPaperTitle","citingPaperYear","isInfluential")
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT ("paperId","citingPaperId") DO UPDATE SET
                        "citingPaperTitle" = EXCLUDED."citingPaperTitle",
                        "citingPaperYear"  = EXCLUDED."citingPaperYear",
                        "isInfluential"    = EXCLUDED."isInfluential"
                    RETURNING id
                ''', (
                    paper_id,
                    citing_paper_id,
                    citing_paper.get('title') if citing_paper else None,
                    citing_paper.get('year')  if citing_paper else None,
                    citation.get('isInfluential', False),
                ))

                citation_id = self.cursor.fetchone()['id']

                for context in citation.get('contexts', []):
                    self.cursor.execute(
                        'INSERT INTO citation_contexts (citation_id, context) VALUES (%s,%s)',
                        (citation_id, context)
                    )

                for intent in citation.get('intents', []):
                    self.cursor.execute(
                        'INSERT INTO citation_intents (citation_id, intent) VALUES (%s,%s)',
                        (citation_id, intent)
                    )

                if citing_paper:
                    for author in citing_paper.get('authors', []):
                        author_name = author.get('name')
                        if author_name:
                            self.cursor.execute('''
                                INSERT INTO citation_authors (citation_id, "authorId", name)
                                VALUES (%s,%s,%s)
                                ON CONFLICT DO NOTHING
                            ''', (citation_id, author.get('authorId'), author_name))

            return True

        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting paper {paper.get('paperId')}: {e}")
            return False

    def insert_publications(self, papers: List[Dict[str, Any]]) -> int:
        """Insert multiple publications and commit."""
        count = sum(1 for paper in papers if self.insert_publication(paper))
        self.conn.commit()
        print(f"✓ Inserted {count}/{len(papers)} publications into database")
        return count

    def load_from_json(self, json_path: str) -> int:
        """Load publications from a JSON file."""
        with open(json_path, 'r', encoding='utf-8') as f:
            papers = json.load(f)
        if isinstance(papers, dict):
            papers = [papers]
        return self.insert_publications(papers)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_publication(self, paper_id: str) -> Optional[Dict]:
        """Get a single publication by ID with all related data."""
        self.cursor.execute('''
            SELECT p.*,
                   e.doi, e.arxiv, e.pubmed, e.dblp,
                   oa.url  AS pdf_url,
                   oa.status AS pdf_status,
                   j.name  AS journal_name,
                   j.volume, j.pages
            FROM publications p
            LEFT JOIN external_ids e  ON p."paperId" = e."paperId"
            LEFT JOIN open_access  oa ON p."paperId" = oa."paperId"
            LEFT JOIN journals     j  ON p."paperId" = j."paperId"
            WHERE p."paperId" = %s
        ''', (paper_id,))

        row = self.cursor.fetchone()
        if not row:
            return None
        paper = dict(row)

        self.cursor.execute('''
            SELECT a.* FROM authors a
            JOIN publication_authors pa ON a."authorId" = pa."authorId"
            WHERE pa."paperId" = %s
        ''', (paper_id,))
        paper['authors'] = [dict(r) for r in self.cursor.fetchall()]

        self.cursor.execute('SELECT type FROM publication_types WHERE "paperId" = %s', (paper_id,))
        paper['publicationTypes'] = [r['type'] for r in self.cursor.fetchall()]

        self.cursor.execute('SELECT field FROM fields_of_study WHERE "paperId" = %s', (paper_id,))
        paper['fieldsOfStudy'] = [r['field'] for r in self.cursor.fetchall()]

        self.cursor.execute('''
            SELECT id, "citingPaperId","citingPaperTitle","citingPaperYear","isInfluential"
            FROM citations WHERE "paperId" = %s
        ''', (paper_id,))

        citations = []
        for crow in self.cursor.fetchall():
            citation_id = crow['id']
            citation = {
                'citingPaperId':    crow['citingPaperId'],
                'citingPaperTitle': crow['citingPaperTitle'],
                'citingPaperYear':  crow['citingPaperYear'],
                'isInfluential':    bool(crow['isInfluential']),
                'contexts': [], 'intents': [], 'authors': [],
            }

            self.cursor.execute(
                'SELECT context FROM citation_contexts WHERE citation_id = %s', (citation_id,))
            citation['contexts'] = [r['context'] for r in self.cursor.fetchall()]

            self.cursor.execute(
                'SELECT intent FROM citation_intents WHERE citation_id = %s', (citation_id,))
            citation['intents'] = [r['intent'] for r in self.cursor.fetchall()]

            self.cursor.execute(
                'SELECT "authorId", name FROM citation_authors WHERE citation_id = %s', (citation_id,))
            citation['authors'] = [dict(r) for r in self.cursor.fetchall()]

            citations.append(citation)

        paper['citations'] = citations
        return paper

    def search_publications(
        self,
        title_contains: str = None,
        year_from: int = None,
        year_to: int = None,
        min_citations: int = None,
        has_doi: bool = None,
        has_open_access: bool = None,
        field_of_study: str = None,
        limit: int = 100,
    ) -> List[Dict]:
        """Search publications with filters."""
        query = '''
            SELECT DISTINCT p.*,
                   e.doi, e.arxiv,
                   oa.url AS pdf_url,
                   j.name AS journal_name
            FROM publications p
            LEFT JOIN external_ids   e ON p."paperId" = e."paperId"
            LEFT JOIN open_access   oa ON p."paperId" = oa."paperId"
            LEFT JOIN journals       j ON p."paperId" = j."paperId"
            LEFT JOIN fields_of_study f ON p."paperId" = f."paperId"
            WHERE 1=1
        '''
        params = []

        if title_contains:
            query += ' AND p.title ILIKE %s'           # ILIKE = case-insensitive in Postgres
            params.append(f'%{title_contains}%')

        if year_from:
            query += ' AND p.year >= %s'
            params.append(year_from)

        if year_to:
            query += ' AND p.year <= %s'
            params.append(year_to)

        if min_citations:
            query += ' AND p."citationCount" >= %s'
            params.append(min_citations)

        if has_doi is not None:
            query += ' AND e.doi IS NOT NULL' if has_doi else ' AND e.doi IS NULL'

        if has_open_access is not None:
            query += ' AND oa.url IS NOT NULL' if has_open_access else ' AND oa.url IS NULL'

        if field_of_study:
            query += ' AND f.field = %s'
            params.append(field_of_study)

        query += ' ORDER BY p."citationCount" DESC LIMIT %s'
        params.append(limit)

        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    # ------------------------------------------------------------------
    # Stats / pipeline helpers  (logic unchanged, only %s placeholders)
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats = {}

        self.cursor.execute('SELECT COUNT(*) FROM publications')
        stats['total_publications'] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT MIN(year), MAX(year) FROM publications WHERE year IS NOT NULL')
        row = self.cursor.fetchone()
        stats['year_range'] = {'min': row['min'], 'max': row['max']}

        self.cursor.execute(
            'SELECT AVG("citationCount"), MAX("citationCount"), SUM("citationCount") FROM publications')
        row = self.cursor.fetchone()
        stats['citation_stats'] = {
            'average': round(float(row['avg']), 2) if row['avg'] else 0,
            'max':     row['max'] or 0,
            'total':   row['sum'] or 0,
        }

        self.cursor.execute(
            "SELECT COUNT(*) FROM publications WHERE abstract IS NOT NULL AND abstract != ''")
        stats['with_abstract'] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM external_ids WHERE doi IS NOT NULL')
        stats['with_doi'] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM open_access WHERE url IS NOT NULL')
        stats['with_open_access'] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM authors')
        stats['total_authors'] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM citations')
        total_citations = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM citations WHERE "isInfluential" = TRUE')
        influential_citations = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM citation_contexts')
        total_contexts = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM citation_intents')
        total_intents = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(DISTINCT "paperId") FROM citations')
        papers_with_citation_data = self.cursor.fetchone()['count']

        stats['citation_context_stats'] = {
            'total_citations_fetched':   total_citations,
            'influential_citations':     influential_citations,
            'total_contexts':            total_contexts,
            'total_intents':             total_intents,
            'papers_with_citation_data': papers_with_citation_data,
            'avg_contexts_per_citation': round(total_contexts / total_citations, 2) if total_citations else 0,
            'avg_intents_per_citation':  round(total_intents  / total_citations, 2) if total_citations else 0,
        }

        self.cursor.execute('''
            SELECT venue, COUNT(*) AS count FROM publications
            WHERE venue IS NOT NULL GROUP BY venue ORDER BY count DESC LIMIT 5
        ''')
        stats['top_venues'] = {r['venue']: r['count'] for r in self.cursor.fetchall()}

        self.cursor.execute('''
            SELECT field, COUNT(*) AS count FROM fields_of_study
            GROUP BY field ORDER BY count DESC LIMIT 5
        ''')
        stats['top_fields'] = {r['field']: r['count'] for r in self.cursor.fetchall()}

        return stats

    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get pipeline processing status statistics."""
        status = {}
        self.cursor.execute('SELECT COUNT(*) FROM publications')
        status['total_papers'] = self.cursor.fetchone()['count']

        for col in ('pdf_downloaded', 'xml_converted', 'sections_extracted', 'features_extracted'):
            self.cursor.execute(f'SELECT COUNT(*) FROM publications WHERE {col} = TRUE')
            status[col] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM publications WHERE pdf_download_error IS NOT NULL')
        status['pdf_errors'] = self.cursor.fetchone()['count']

        self.cursor.execute('SELECT COUNT(*) FROM publications WHERE xml_conversion_error IS NOT NULL')
        status['xml_errors'] = self.cursor.fetchone()['count']

        return status

    def get_papers_needing_download(self, limit: int = None) -> List[Dict]:
        """Get papers that need PDF download."""
        query = '''
            SELECT p."paperId", p.title, oa.url
            FROM publications p
            JOIN open_access oa ON p."paperId" = oa."paperId"
            WHERE oa.url IS NOT NULL
              AND (p.pdf_downloaded = FALSE OR p.pdf_downloaded IS NULL)
        '''
        if limit:
            query += f' LIMIT {limit}'
        self.cursor.execute(query)
        return [dict(r) for r in self.cursor.fetchall()]

    def get_papers_needing_conversion(self, limit: int = None) -> List[Dict]:
        """Get papers that need XML conversion."""
        query = '''
            SELECT "paperId", title, pdf_path FROM publications
            WHERE pdf_downloaded = TRUE
              AND pdf_path IS NOT NULL
              AND (xml_converted = FALSE OR xml_converted IS NULL)
        '''
        if limit:
            query += f' LIMIT {limit}'
        self.cursor.execute(query)
        return [dict(r) for r in self.cursor.fetchall()]

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def clear_db(self):
        """Delete all rows but keep table structure."""
        tables = [
            'citation_authors', 'citation_intents', 'citation_contexts', 'citations',
            'fields_of_study', 'publication_types', 'journals', 'open_access',
            'external_ids', 'publication_authors', 'authors', 'publications',
        ]
        for table in tables:
            self.cursor.execute(f'DELETE FROM {table}')
        self.conn.commit()
        print("✓ Database cleared")

    def drop_tables(self):
        """Drop all tables and recreate them."""
        tables = [
            'citation_authors', 'citation_intents', 'citation_contexts', 'citations',
            'fields_of_study', 'publication_types', 'journals', 'open_access',
            'external_ids', 'publication_authors', 'authors', 'publications',
        ]
        for table in tables:
            self.cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
        self.conn.commit()
        print("✓ All tables dropped")
        self._create_tables()

    def reset_database(self, confirm: bool = False):
        """Drop and recreate all tables (deletes ALL data)."""
        if not confirm:
            print("WARNING: This will delete ALL data! Call reset_database(confirm=True) to proceed.")
            return
        print("\nResetting database...")
        self.cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        for row in self.cursor.fetchall():
            self.cursor.execute(f'DROP TABLE IF EXISTS {row["tablename"]} CASCADE')
        self.conn.commit()
        self._create_tables()
        print("Database reset complete")

    def reset_pipeline_status(self):
        """Reset pipeline tracking columns; delete PDF/XML files from disk."""
        print("\nResetting pipeline status...")

        self.cursor.execute('SELECT pdf_path FROM publications WHERE pdf_path IS NOT NULL')
        pdf_paths = [r['pdf_path'] for r in self.cursor.fetchall()]

        self.cursor.execute('SELECT xml_path FROM publications WHERE xml_path IS NOT NULL')
        xml_paths = [r['xml_path'] for r in self.cursor.fetchall()]

        def _delete_files(paths):
            deleted = not_found = 0
            for p in paths:
                try:
                    path = Path(p)
                    if path.exists():
                        path.unlink(); deleted += 1
                    else:
                        not_found += 1
                except Exception as e:
                    print(f"Warning: could not delete {p}: {e}")
            return deleted, not_found

        pdf_del, pdf_nf = _delete_files(pdf_paths)
        xml_del, xml_nf = _delete_files(xml_paths)

        xml_output_dir = Path(__file__).parent.parent.parent / 'outputs' / 'xml'
        if xml_output_dir.exists():
            for xml_file in xml_output_dir.glob("*.tei.xml"):
                try:
                    xml_file.unlink(); xml_del += 1
                except Exception as e:
                    print(f"Warning: could not delete {xml_file}: {e}")

        self.cursor.execute('''
            UPDATE publications SET
                pdf_downloaded = FALSE, pdf_download_date = NULL,
                pdf_path = NULL, pdf_download_error = NULL,
                xml_converted = FALSE, xml_conversion_date = NULL,
                xml_path = NULL, xml_conversion_error = NULL,
                sections_extracted = FALSE, sections_extraction_date = NULL,
                sections_extraction_error = NULL,
                features_extracted = FALSE, features_extraction_date = NULL,
                features_extraction_error = NULL,
                updated_at = CURRENT_TIMESTAMP
        ''')
        rows_updated = self.cursor.rowcount
        self.conn.commit()

        print(f"\nPipeline Reset Complete:")
        print(f"  Records updated : {rows_updated}")
        print(f"  PDFs deleted    : {pdf_del}  (not found: {pdf_nf})")
        print(f"  XMLs deleted    : {xml_del}  (not found: {xml_nf})")

    def export_to_json(self, output_path: str, limit: int = None):
        """Export publications to JSON."""
        query = 'SELECT "paperId" FROM publications'
        if limit:
            query += f' LIMIT {limit}'
        self.cursor.execute(query)
        paper_ids = [r['paperId'] for r in self.cursor.fetchall()]
        papers = [self.get_publication(pid) for pid in paper_ids]
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(papers, f, indent=2, ensure_ascii=False, default=str)
        print(f"✓ Exported {len(papers)} publications to {output_path}")

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.close()