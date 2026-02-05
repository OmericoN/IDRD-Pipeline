import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
from datetime import datetime


class PublicationDatabase:
    """SQLite database manager for storing publication metadata."""
    
    def __init__(self, db_path: str = None):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file. Defaults to schema/publications.db
        """
        if db_path is None:
            db_dir = Path(__file__).parent
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / 'publications.db'
        
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        self.cursor = self.conn.cursor()
        
        self._create_tables()
        print(f"✓ Database initialized at {self.db_path}")
    
    def _create_tables(self):
        """Create database tables if they don't exist."""
        
        # Main publications table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publications (
                paperId TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                abstract TEXT,
                year INTEGER,
                url TEXT,
                venue TEXT,
                publicationDate TEXT,
                citationCount INTEGER DEFAULT 0,
                referenceCount INTEGER DEFAULT 0,
                influentialCitationCount INTEGER DEFAULT 0,
                tldr TEXT,
                isOpenAccess BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Authors table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS authors (
                authorId TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Paper-Author relationship (many-to-many)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publication_authors (
                paperId TEXT,
                authorId TEXT,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE,
                FOREIGN KEY (authorId) REFERENCES authors(authorId) ON DELETE CASCADE,
                PRIMARY KEY (paperId, authorId)
            )
        ''')
        
        # External IDs table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS external_ids (
                paperId TEXT PRIMARY KEY,
                doi TEXT,
                arxiv TEXT,
                pubmed TEXT,
                dblp TEXT,
                corpusId TEXT,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE
            )
        ''')
        
        # Open Access table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS open_access (
                paperId TEXT PRIMARY KEY,
                url TEXT,
                status TEXT,
                license TEXT,
                disclaimer TEXT,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE
            )
        ''')
        
        # Journal table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS journals (
                paperId TEXT PRIMARY KEY,
                name TEXT,
                volume TEXT,
                pages TEXT,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE
            )
        ''')
        
        # Publication types table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS publication_types (
                paperId TEXT,
                type TEXT,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE,
                PRIMARY KEY (paperId, type)
            )
        ''')
        
        # Fields of study table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fields_of_study (
                paperId TEXT,
                field TEXT,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE,
                PRIMARY KEY (paperId, field)
            )
        ''')
        
        # Citations table - NEW
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                paperId TEXT NOT NULL,
                citingPaperId TEXT,
                citingPaperTitle TEXT,
                citingPaperYear INTEGER,
                isInfluential BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (paperId) REFERENCES publications(paperId) ON DELETE CASCADE,
                UNIQUE(paperId, citingPaperId)
            )
        ''')
        
        # Citation contexts table - NEW
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citation_contexts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                citation_id INTEGER NOT NULL,
                context TEXT NOT NULL,
                FOREIGN KEY (citation_id) REFERENCES citations(id) ON DELETE CASCADE
            )
        ''')
        
        # Citation intents table - NEW
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citation_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                citation_id INTEGER NOT NULL,
                intent TEXT NOT NULL,
                FOREIGN KEY (citation_id) REFERENCES citations(id) ON DELETE CASCADE
            )
        ''')
        
        # Citation authors table - NEW
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS citation_authors (
                citation_id INTEGER NOT NULL,
                authorId TEXT,
                name TEXT NOT NULL,
                FOREIGN KEY (citation_id) REFERENCES citations(id) ON DELETE CASCADE,
                PRIMARY KEY (citation_id, authorId)
            )
        ''')
        
        # Create indexes for faster queries
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_year ON publications(year)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_citations ON publications(citationCount)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON publications(title)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_doi ON external_ids(doi)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_citing_paper ON citations(citingPaperId)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_citation_paper ON citations(paperId)')
        
        self.conn.commit()
    
    def insert_publication(self, paper: Dict[str, Any]) -> bool:
        """
        Insert or update a single publication.
        
        Args:
            paper: Parsed paper dictionary from PaperDictParser
            
        Returns:
            True if successful
        """
        try:
            # Insert main publication data
            self.cursor.execute('''
                INSERT OR REPLACE INTO publications 
                (paperId, title, abstract, year, url, venue, publicationDate,
                 citationCount, referenceCount, influentialCitationCount, tldr, 
                 isOpenAccess, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
                paper.get('tldr'),
                paper.get('isOpenAccess', False)
            ))
            
            paper_id = paper.get('paperId')
            
            # Insert authors
            for author in paper.get('authors', []):
                author_id = author.get('authorId')
                if author_id:
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO authors (authorId, name, url)
                        VALUES (?, ?, ?)
                    ''', (
                        author_id,
                        author.get('name'),
                        author.get('url')
                    ))
                    
                    # Link paper and author
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO publication_authors (paperId, authorId)
                        VALUES (?, ?)
                    ''', (paper_id, author_id))
            
            # Insert external IDs
            ext_ids = paper.get('externalIds', {})
            if ext_ids:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO external_ids 
                    (paperId, doi, arxiv, pubmed, dblp, corpusId)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    paper_id,
                    ext_ids.get('DOI'),
                    ext_ids.get('ArXiv'),
                    ext_ids.get('PubMed'),
                    ext_ids.get('DBLP'),
                    ext_ids.get('CorpusId')
                ))
            
            # Insert open access info
            oa = paper.get('openAccessPdf')
            if oa:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO open_access 
                    (paperId, url, status, license, disclaimer)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    paper_id,
                    oa.get('url'),
                    oa.get('status'),
                    oa.get('license'),
                    oa.get('disclaimer')
                ))
            
            # Insert journal info
            journal = paper.get('journal')
            if journal:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO journals (paperId, name, volume, pages)
                    VALUES (?, ?, ?, ?)
                ''', (
                    paper_id,
                    journal.get('name'),
                    journal.get('volume'),
                    journal.get('pages')
                ))
            
            # Insert publication types
            for pub_type in paper.get('publicationTypes', []):
                self.cursor.execute('''
                    INSERT OR IGNORE INTO publication_types (paperId, type)
                    VALUES (?, ?)
                ''', (paper_id, pub_type))
            
            # Insert fields of study
            for field in paper.get('fieldsOfStudy', []):
                self.cursor.execute('''
                    INSERT OR IGNORE INTO fields_of_study (paperId, field)
                    VALUES (?, ?)
                ''', (paper_id, field))
            
            # Insert citations with contexts and intents - NEW
            for citation in paper.get('citations', []):
                citing_paper = citation.get('citingPaper', {})
                citing_paper_id = citing_paper.get('paperId') if citing_paper else None
                
                # Insert citation
                self.cursor.execute('''
                    INSERT OR REPLACE INTO citations 
                    (paperId, citingPaperId, citingPaperTitle, citingPaperYear, isInfluential)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    paper_id,
                    citing_paper_id,
                    citing_paper.get('title') if citing_paper else None,
                    citing_paper.get('year') if citing_paper else None,
                    citation.get('isInfluential', False)
                ))
                
                # Get the citation_id
                citation_id = self.cursor.lastrowid
                
                # Insert contexts
                for context in citation.get('contexts', []):
                    self.cursor.execute('''
                        INSERT INTO citation_contexts (citation_id, context)
                        VALUES (?, ?)
                    ''', (citation_id, context))
                
                # Insert intents
                for intent in citation.get('intents', []):
                    self.cursor.execute('''
                        INSERT INTO citation_intents (citation_id, intent)
                        VALUES (?, ?)
                    ''', (citation_id, intent))
                
                # Insert citing paper authors
                if citing_paper:
                    for author in citing_paper.get('authors', []):
                        author_id = author.get('authorId')
                        author_name = author.get('name')
                        if author_name:
                            self.cursor.execute('''
                                INSERT OR IGNORE INTO citation_authors 
                                (citation_id, authorId, name)
                                VALUES (?, ?, ?)
                            ''', (citation_id, author_id, author_name))
            
            return True
            
        except Exception as e:
            print(f"Error inserting paper {paper.get('paperId')}: {e}")
            return False
    
    def insert_publications(self, papers: List[Dict[str, Any]]) -> int:
        """
        Insert multiple publications.
        
        Args:
            papers: List of parsed paper dictionaries
            
        Returns:
            Number of papers successfully inserted
        """
        count = 0
        for paper in papers:
            if self.insert_publication(paper):
                count += 1
        
        self.conn.commit()
        print(f"✓ Inserted {count}/{len(papers)} publications into database")
        return count
    
    def load_from_json(self, json_path: str) -> int:
        """
        Load publications from JSON file into database.
        
        Args:
            json_path: Path to JSON file with publications
            
        Returns:
            Number of publications loaded
        """
        with open(json_path, 'r', encoding='utf-8') as f:
            papers = json.load(f)
        
        if isinstance(papers, dict):
            papers = [papers]
        
        return self.insert_publications(papers)
    
    def get_publication(self, paper_id: str) -> Optional[Dict]:
        """Get a single publication by ID with all related data."""
        self.cursor.execute('''
            SELECT p.*, 
                   e.doi, e.arxiv, e.pubmed, e.dblp,
                   oa.url as pdf_url, oa.status as pdf_status,
                   j.name as journal_name, j.volume, j.pages
            FROM publications p
            LEFT JOIN external_ids e ON p.paperId = e.paperId
            LEFT JOIN open_access oa ON p.paperId = oa.paperId
            LEFT JOIN journals j ON p.paperId = j.paperId
            WHERE p.paperId = ?
        ''', (paper_id,))
        
        row = self.cursor.fetchone()
        if not row:
            return None
        
        paper = dict(row)
        
        # Get authors
        self.cursor.execute('''
            SELECT a.* FROM authors a
            JOIN publication_authors pa ON a.authorId = pa.authorId
            WHERE pa.paperId = ?
        ''', (paper_id,))
        paper['authors'] = [dict(r) for r in self.cursor.fetchall()]
        
        # Get publication types
        self.cursor.execute('''
            SELECT type FROM publication_types WHERE paperId = ?
        ''', (paper_id,))
        paper['publicationTypes'] = [r[0] for r in self.cursor.fetchall()]
        
        # Get fields of study
        self.cursor.execute('''
            SELECT field FROM fields_of_study WHERE paperId = ?
        ''', (paper_id,))
        paper['fieldsOfStudy'] = [r[0] for r in self.cursor.fetchall()]
        
        # Get citations with contexts and intents - NEW
        self.cursor.execute('''
            SELECT id, citingPaperId, citingPaperTitle, citingPaperYear, isInfluential
            FROM citations
            WHERE paperId = ?
        ''', (paper_id,))
        
        citations = []
        for citation_row in self.cursor.fetchall():
            citation_id = citation_row[0]
            citation = {
                'citingPaperId': citation_row[1],
                'citingPaperTitle': citation_row[2],
                'citingPaperYear': citation_row[3],
                'isInfluential': bool(citation_row[4]),
                'contexts': [],
                'intents': [],
                'authors': []
            }
            
            # Get contexts
            self.cursor.execute('''
                SELECT context FROM citation_contexts WHERE citation_id = ?
            ''', (citation_id,))
            citation['contexts'] = [r[0] for r in self.cursor.fetchall()]
            
            # Get intents
            self.cursor.execute('''
                SELECT intent FROM citation_intents WHERE citation_id = ?
            ''', (citation_id,))
            citation['intents'] = [r[0] for r in self.cursor.fetchall()]
            
            # Get authors
            self.cursor.execute('''
                SELECT authorId, name FROM citation_authors WHERE citation_id = ?
            ''', (citation_id,))
            citation['authors'] = [{'authorId': r[0], 'name': r[1]} for r in self.cursor.fetchall()]
            
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
        limit: int = 100
    ) -> List[Dict]:
        """
        Search publications with filters.
        
        Returns:
            List of matching publications
        """
        query = '''
            SELECT DISTINCT p.*, 
                   e.doi, e.arxiv,
                   oa.url as pdf_url,
                   j.name as journal_name
            FROM publications p
            LEFT JOIN external_ids e ON p.paperId = e.paperId
            LEFT JOIN open_access oa ON p.paperId = oa.paperId
            LEFT JOIN journals j ON p.paperId = j.paperId
            LEFT JOIN fields_of_study f ON p.paperId = f.paperId
            WHERE 1=1
        '''
        params = []
        
        if title_contains:
            query += ' AND p.title LIKE ?'
            params.append(f'%{title_contains}%')
        
        if year_from:
            query += ' AND p.year >= ?'
            params.append(year_from)
        
        if year_to:
            query += ' AND p.year <= ?'
            params.append(year_to)
        
        if min_citations:
            query += ' AND p.citationCount >= ?'
            params.append(min_citations)
        
        if has_doi is not None:
            if has_doi:
                query += ' AND e.doi IS NOT NULL'
            else:
                query += ' AND e.doi IS NULL'
        
        if has_open_access is not None:
            if has_open_access:
                query += ' AND oa.url IS NOT NULL'
            else:
                query += ' AND oa.url IS NULL'
        
        if field_of_study:
            query += ' AND f.field = ?'
            params.append(field_of_study)
        
        query += ' ORDER BY p.citationCount DESC LIMIT ?'
        params.append(limit)
        
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats = {}
        
        # Total publications
        self.cursor.execute('SELECT COUNT(*) FROM publications')
        stats['total_publications'] = self.cursor.fetchone()[0]
        
        # Year range
        self.cursor.execute('''
            SELECT MIN(year), MAX(year) FROM publications 
            WHERE year IS NOT NULL
        ''')
        min_year, max_year = self.cursor.fetchone()
        stats['year_range'] = {'min': min_year, 'max': max_year}
        
        # Citation stats
        self.cursor.execute('''
            SELECT AVG(citationCount), MAX(citationCount), 
                   SUM(citationCount)
            FROM publications
        ''')
        avg_cit, max_cit, total_cit = self.cursor.fetchone()
        stats['citation_stats'] = {
            'average': round(avg_cit, 2) if avg_cit else 0,
            'max': max_cit or 0,
            'total': total_cit or 0
        }
        
        # Publications with abstracts
        self.cursor.execute('''
            SELECT COUNT(*) FROM publications 
            WHERE abstract IS NOT NULL AND abstract != ''
        ''')
        stats['with_abstract'] = self.cursor.fetchone()[0]
        
        # Publications with DOI
        self.cursor.execute('''
            SELECT COUNT(*) FROM external_ids 
            WHERE doi IS NOT NULL
        ''')
        stats['with_doi'] = self.cursor.fetchone()[0]
        
        # Publications with open access
        self.cursor.execute('''
            SELECT COUNT(*) FROM open_access 
            WHERE url IS NOT NULL
        ''')
        stats['with_open_access'] = self.cursor.fetchone()[0]
        
        # Total authors
        self.cursor.execute('SELECT COUNT(*) FROM authors')
        stats['total_authors'] = self.cursor.fetchone()[0]
        
        # Citation context statistics - NEW
        self.cursor.execute('SELECT COUNT(*) FROM citations')
        total_citations = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM citations WHERE isInfluential = 1
        ''')
        influential_citations = self.cursor.fetchone()[0]
        
        self.cursor.execute('SELECT COUNT(*) FROM citation_contexts')
        total_contexts = self.cursor.fetchone()[0]
        
        self.cursor.execute('SELECT COUNT(*) FROM citation_intents')
        total_intents = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(DISTINCT paperId) FROM citations
        ''')
        papers_with_citation_data = self.cursor.fetchone()[0]
        
        stats['citation_context_stats'] = {
            'total_citations_fetched': total_citations,
            'influential_citations': influential_citations,
            'total_contexts': total_contexts,
            'total_intents': total_intents,
            'papers_with_citation_data': papers_with_citation_data,
            'avg_contexts_per_citation': round(total_contexts / total_citations, 2) if total_citations > 0 else 0,
            'avg_intents_per_citation': round(total_intents / total_citations, 2) if total_citations > 0 else 0
        }
        
        # Top venues
        self.cursor.execute('''
            SELECT venue, COUNT(*) as count 
            FROM publications 
            WHERE venue IS NOT NULL 
            GROUP BY venue 
            ORDER BY count DESC 
            LIMIT 5
        ''')
        stats['top_venues'] = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        # Top fields of study
        self.cursor.execute('''
            SELECT field, COUNT(*) as count 
            FROM fields_of_study 
            GROUP BY field 
            ORDER BY count DESC 
            LIMIT 5
        ''')
        stats['top_fields'] = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        return stats
    
    def clear_db(self):
        """Clear all data from the database (keeps table structure)."""
        tables = [
            'citation_authors',
            'citation_intents',
            'citation_contexts',
            'citations',
            'fields_of_study',
            'publication_types',
            'journals',
            'open_access',
            'external_ids',
            'publication_authors',
            'authors',
            'publications'
        ]
        
        for table in tables:
            self.cursor.execute(f'DELETE FROM {table}')
        
        self.conn.commit()
        print("✓ Database cleared (all rows deleted)")
    
    def drop_tables(self):
        """Drop all tables (complete reset)."""
        tables = [
            'citation_authors',
            'citation_intents',
            'citation_contexts',
            'citations',
            'fields_of_study',
            'publication_types',
            'journals',
            'open_access',
            'external_ids',
            'publication_authors',
            'authors',
            'publications'
        ]
        
        for table in tables:
            self.cursor.execute(f'DROP TABLE IF EXISTS {table}')
        
        self.conn.commit()
        print("✓ All tables dropped")
        
        # Recreate tables
        self._create_tables()
    
    def export_to_json(self, output_path: str, limit: int = None):
        """Export all publications to JSON."""
        query = 'SELECT paperId FROM publications'
        if limit:
            query += f' LIMIT {limit}'
        
        self.cursor.execute(query)
        paper_ids = [row[0] for row in self.cursor.fetchall()]
        
        papers = [self.get_publication(pid) for pid in paper_ids]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(papers, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Exported {len(papers)} publications to {output_path}")
    
    def commit(self):
        """Commit pending transactions."""
        self.conn.commit()
    
    def close(self):
        """Close database connection."""
        self.conn.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.commit()
        self.close()


# Example usage
if __name__ == "__main__":
    # Initialize database
    db = PublicationDatabase()
    
    # Load publications from JSON
    json_path = Path(__file__).parent.parent.parent / 'outputs' / 'retrieved_results.json'
    if json_path.exists():
        print(f"Loading publications from {json_path}...")
        count = db.load_from_json(str(json_path))
        print(f"✓ Loaded {count} publications")
        
        # Print statistics
        print("\n" + "="*50)
        print("DATABASE STATISTICS")
        print("="*50)
        stats = db.get_statistics()
        print(json.dumps(stats, indent=2))
        
        # Example searches
        print("\n" + "="*50)
        print("EXAMPLE QUERIES")
        print("="*50)
        
        # Highly cited papers
        highly_cited = db.search_publications(min_citations=1000, limit=5)
        print(f"\nHighly cited papers (>1000 citations): {len(highly_cited)}")
        for paper in highly_cited[:3]:
            print(f"  - {paper['title']} ({paper['citationCount']} citations)")
        
        # Recent papers with open access
        recent_oa = db.search_publications(
            year_from=2022,
            has_open_access=True,
            limit=5
        )
        print(f"\nRecent open access papers (2022+): {len(recent_oa)}")
        for paper in recent_oa[:3]:
            print(f"  - {paper['title']} ({paper['year']})")
        
        # Papers in Computer Science
        cs_papers = db.search_publications(
            field_of_study="Computer Science",
            limit=5
        )
        print(f"\nComputer Science papers: {len(cs_papers)}")
    
    else:
        print(f"No JSON file found at {json_path}")
        print("Run fetching.py first to retrieve publications")
    
    db.close()