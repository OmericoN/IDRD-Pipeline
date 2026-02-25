from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import os

load_dotenv()


class PublicationDatabase:
    """PostgreSQL database manager for the IDRD Pipeline."""

    _init_count = 0   # class-level counter to detect accidental double-instantiation

    def __init__(self):
        PublicationDatabase._init_count += 1
        if PublicationDatabase._init_count > 1:
            import traceback
            print(f"\n⚠  WARNING: PublicationDatabase instantiated {PublicationDatabase._init_count} times.")
            print("   Stack trace — pinpoints the extra instantiation:")
            traceback.print_stack(limit=8)

        self.conn = psycopg2.connect(
            host     = os.getenv("POSTGRES_HOST",     "localhost"),
            port     = os.getenv("POSTGRES_PORT",     "5432"),
            dbname   = os.getenv("POSTGRES_DB",       "idrd_pipeline"),
            user     = os.getenv("POSTGRES_USER",     "postgres"),
            password = os.getenv("POSTGRES_PASSWORD", ""),
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        self._create_tables()
        print("✓ PostgreSQL database initialized")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_tables(self):
        """Create database tables if they don't exist."""

        # publications
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publications (
                "paperId"                   TEXT PRIMARY KEY,
                title                       TEXT,
                abstract                    TEXT,
                year                        INTEGER,
                citation_count              INTEGER DEFAULT 0,
                reference_count             INTEGER DEFAULT 0,
                influential_citation_count  INTEGER DEFAULT 0,
                venue                       TEXT,
                publication_date            DATE,
                publication_types           TEXT[],
                journal_name                TEXT,
                journal_volume              TEXT,
                journal_pages               TEXT,
                fields_of_study             TEXT[],
                url                         TEXT,
                doi                         TEXT,
                is_open_access              BOOLEAN DEFAULT FALSE,
                open_access_pdf_url         TEXT,
                open_access_pdf_status      TEXT,
                tldr                        TEXT,
                pdf_downloaded              BOOLEAN DEFAULT FALSE,
                pdf_download_date           TIMESTAMP,
                pdf_path                    TEXT,
                pdf_download_error          TEXT,
                xml_converted               BOOLEAN DEFAULT FALSE,
                xml_conversion_date         TIMESTAMP,
                xml_path                    TEXT,
                xml_conversion_error        TEXT,
                sections_extracted          BOOLEAN DEFAULT FALSE,
                features_extracted          BOOLEAN DEFAULT FALSE,
                created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # authors
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS authors (
                id          SERIAL PRIMARY KEY,
                "authorId"  TEXT UNIQUE,
                name        TEXT NOT NULL
            )
        ''')

        # publication_authors join table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publication_authors (
                publication_id  TEXT NOT NULL REFERENCES publications("paperId") ON DELETE CASCADE,
                author_id       INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
                author_order    INTEGER,
                PRIMARY KEY (publication_id, author_id)
            )
        ''')

        self.cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_publications_year ON publications(year)'
        )
        self.cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_publications_pdf ON publications(pdf_downloaded)'
        )
        self.cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_publications_xml ON publications(xml_converted)'
        )

        self.conn.commit()
        print("✓ PostgreSQL database initialized")

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
            for citation in paper.get('citations', []) or []:   # guard against None
                citing_paper = citation.get('citingPaper') or {}
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
                    for author in citing_paper.get('authors', []) or []:  # guard against None
                        author_name = author.get('name')
                        if author_name:
                            self.cursor.execute('''
                                INSERT INTO citation_authors (citation_id, "authorId", name)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (citation_id, name) DO NOTHING
                            ''', (citation_id, author.get('authorId'), author_name))

            return True

        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting paper {paper.get('paperId')}: {e}")
            return False

    def insert_publications(self, papers: List[Dict]) -> int:
        """Insert publications and their authors. Citations removed."""
        count = 0
        for paper in papers:
            try:
                paper_id = paper.get('paperId')
                if not paper_id:
                    continue

                external_ids = paper.get('externalIds') or {}
                open_access  = paper.get('openAccessPdf') or {}
                journal      = paper.get('journal') or {}
                tldr         = paper.get('tldr') or {}

                self.cursor.execute('''
                    INSERT INTO publications (
                        "paperId", title, abstract, year,
                        citation_count, reference_count, influential_citation_count,
                        venue, publication_date, publication_types,
                        journal_name, journal_volume, journal_pages,
                        fields_of_study, url, doi,
                        is_open_access, open_access_pdf_url, open_access_pdf_status,
                        tldr
                    ) VALUES (
                        %s,%s,%s,%s, %s,%s,%s, %s,%s,%s,
                        %s,%s,%s, %s,%s,%s, %s,%s,%s, %s
                    )
                    ON CONFLICT ("paperId") DO UPDATE SET
                        title                      = EXCLUDED.title,
                        citation_count             = EXCLUDED.citation_count,
                        updated_at                 = CURRENT_TIMESTAMP
                ''', (
                    paper_id,
                    paper.get('title'),
                    paper.get('abstract'),
                    paper.get('year'),
                    paper.get('citationCount', 0),
                    paper.get('referenceCount', 0),
                    paper.get('influentialCitationCount', 0),
                    paper.get('venue'),
                    paper.get('publicationDate'),
                    paper.get('publicationTypes'),
                    journal.get('name'),
                    journal.get('volume'),
                    journal.get('pages'),
                    paper.get('fieldsOfStudy'),
                    paper.get('url'),
                    external_ids.get('DOI'),
                    paper.get('isOpenAccess', False),
                    open_access.get('url'),
                    open_access.get('status'),
                    tldr.get('text'),
                ))

                # authors
                for author in paper.get('authors') or []:
                    author_id   = author.get('authorId')
                    author_name = author.get('name')
                    if not author_name:
                        continue

                    if author_id:
                        self.cursor.execute('''
                            INSERT INTO authors ("authorId", name)
                            VALUES (%s, %s)
                            ON CONFLICT ("authorId") DO UPDATE SET name = EXCLUDED.name
                        ''', (author_id, author_name))
                        self.cursor.execute(
                            'SELECT id FROM authors WHERE "authorId" = %s', (author_id,)
                        )
                    else:
                        self.cursor.execute('''
                            INSERT INTO authors ("authorId", name)
                            VALUES (NULL, %s)
                            ON CONFLICT DO NOTHING
                        ''', (author_name,))
                        self.cursor.execute(
                            'SELECT id FROM authors WHERE name = %s AND "authorId" IS NULL',
                            (author_name,)
                        )

                    row = self.cursor.fetchone()
                    if row:
                        self.cursor.execute('''
                            INSERT INTO publication_authors (publication_id, author_id, author_order)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        ''', (paper_id, row['id'], None))

                self.conn.commit()
                count += 1

            except Exception as e:
                self.conn.rollback()
                print(f"Error inserting paper {paper.get('paperId')}: {e}")

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

    def get_pipeline_status(self) -> Dict:
        """Return counts for each pipeline stage."""
        self.cursor.execute("""
            SELECT
                COUNT(*)                                            AS total_papers,
                COUNT(*) FILTER (WHERE pdf_downloaded = TRUE)      AS pdf_downloaded,
                COUNT(*) FILTER (WHERE xml_converted  = TRUE)      AS xml_converted,
                COUNT(*) FILTER (WHERE sections_extracted = TRUE)  AS sections_extracted,
                COUNT(*) FILTER (WHERE features_extracted = TRUE)  AS features_extracted,
                COUNT(*) FILTER (WHERE pdf_download_error IS NOT NULL
                                   AND pdf_download_error != '')   AS pdf_errors,
                COUNT(*) FILTER (WHERE xml_conversion_error IS NOT NULL
                                   AND xml_conversion_error != '') AS xml_errors
            FROM publications
        """)
        row = self.cursor.fetchone()
        return dict(row) if row else {}

    def get_papers_needing_download(self, limit: int = None) -> List[Dict]:
        """Return papers that have a PDF URL but haven't been downloaded yet."""
        query = """
            SELECT
                p."paperId",
                p.title,
                p.open_access_pdf_url  AS url
            FROM publications p
            WHERE p.open_access_pdf_url IS NOT NULL
              AND p.open_access_pdf_url != ''
              AND (p.pdf_downloaded IS FALSE OR p.pdf_downloaded IS NULL)
              AND (p.pdf_download_error IS NULL OR p.pdf_download_error = '')
        """
        if limit:
            query += f" LIMIT {limit}"

        self.cursor.execute(query)
        return self.cursor.fetchall()

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
        """Drop and recreate all tables AND delete all data/log files."""
        if not confirm:
            print("WARNING: Call reset_database(confirm=True) to proceed.")
            return

        print("\nPerforming full database reset...")

        pdf_dir      = Path(__file__).parent.parent.parent / 'data' / 'pdf'
        xml_dir      = Path(__file__).parent.parent.parent / 'data' / 'xml'
        markdown_dir = Path(__file__).parent.parent.parent / 'data' / 'markdown'
        runs_dir     = Path(__file__).parent.parent.parent / 'logs' / 'runs'

        pdf_deleted      = self._clear_directory(pdf_dir)
        xml_deleted      = self._clear_directory(xml_dir)
        markdown_deleted = self._clear_directory(markdown_dir)
        json_deleted     = self._clear_directory(runs_dir)

        print(f"  Deleted {pdf_deleted}      PDF files      from {pdf_dir}")
        print(f"  Deleted {xml_deleted}      XML files      from {xml_dir}")
        print(f"  Deleted {markdown_deleted} Markdown files from {markdown_dir}")
        print(f"  Deleted {json_deleted}     log files      from {runs_dir}")

        self.cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        for row in self.cursor.fetchall():
            self.cursor.execute(f'DROP TABLE IF EXISTS {row["tablename"]} CASCADE')
        self.conn.commit()
        print("  Dropped all tables")

        self._create_tables()
        print("✓ Full database reset complete")

    @staticmethod
    def _clear_directory(directory: Path) -> int:
        """Delete all files in a directory recursively. Returns count deleted."""
        deleted = 0
        if not directory.exists():
            return 0
        for f in directory.rglob("*"):
            if f.is_file():
                f.unlink(missing_ok=True)
                deleted += 1
        return deleted

    def reset_pipeline_status(self):
        """Reset pipeline tracking columns and delete all data files."""
        print("\nResetting pipeline status...")

        pdf_dir      = Path(__file__).parent.parent.parent / 'data' / 'pdf'
        xml_dir      = Path(__file__).parent.parent.parent / 'data' / 'xml'
        markdown_dir = Path(__file__).parent.parent.parent / 'data' / 'markdown'
        runs_dir     = Path(__file__).parent.parent.parent / 'logs' / 'runs'

        pdf_deleted      = self._clear_directory(pdf_dir)
        xml_deleted      = self._clear_directory(xml_dir)
        markdown_deleted = self._clear_directory(markdown_dir)
        json_deleted     = self._clear_directory(runs_dir)

        print(f"  Deleted {pdf_deleted}      PDF files      from {pdf_dir}")
        print(f"  Deleted {xml_deleted}      XML files      from {xml_dir}")
        print(f"  Deleted {markdown_deleted} Markdown files from {markdown_dir}")
        print(f"  Deleted {json_deleted}     log files      from {runs_dir}")

        self.cursor.execute('''
            UPDATE publications SET
                pdf_downloaded       = FALSE,
                pdf_download_date    = NULL,
                pdf_path             = NULL,
                pdf_download_error   = NULL,
                xml_converted        = FALSE,
                xml_conversion_date  = NULL,
                xml_path             = NULL,
                xml_conversion_error = NULL,
                sections_extracted   = FALSE,
                features_extracted   = FALSE,
                updated_at           = CURRENT_TIMESTAMP
        ''')
        self.conn.commit()
        print(f"  Reset tracking columns for {self.cursor.rowcount} publications")
        print("✓ Pipeline status reset complete")

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