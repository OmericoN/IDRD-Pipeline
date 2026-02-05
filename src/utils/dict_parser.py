import json
from typing import Dict, List, Any, Optional
from pathlib import Path
import pandas as pd
import os


class PaperDictParser:
    """
    Parser for converting Semantic Scholar API paper dictionaries to structured JSON.
    
    Handles nested structures, optional fields, and provides multiple output formats.
    """
    
    def __init__(self, output_dir: str = None):
        """
        Initialize the parser.
        
        Args:
            output_dir: Directory to save output files. Defaults to 'outputs' folder.
        """
        self.parsed_papers = []
        
        # Set output directory
        if output_dir is None:
            # Default to outputs folder in project root
            project_root = Path(__file__).parent.parent.parent
            self.output_dir = project_root / 'outputs'
        else:
            self.output_dir = Path(output_dir)
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def parse_paper(self, paper: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single paper dictionary into a clean, structured format.
        
        Args:
            paper: Raw paper dictionary from API
            
        Returns:
            Cleaned and structured paper dictionary
        """
        # Determine open access status
        # Priority: 1. Check explicit isOpenAccess field, 2. Check if PDF exists with GREEN/GOLD/HYBRID/BRONZE status
        is_open_access = paper.get('isOpenAccess', False)
        
        # If not explicitly marked, check PDF status
        if not is_open_access:
            pdf_info = paper.get('openAccessPdf', {})
            if pdf_info and pdf_info.get('url'):
                status = pdf_info.get('status', '').upper()
                # GREEN, GOLD, HYBRID, BRONZE all indicate open access
                is_open_access = status in ['GREEN', 'GOLD', 'HYBRID', 'BRONZE']
        
        return {
            'paperId': paper.get('paperId'),
            'title': paper.get('title'),
            'year': paper.get('year'),
            'abstract': paper.get('abstract'),
            'url': paper.get('url'),
            'venue': paper.get('venue'),
            
            # Publication info
            'publicationDate': paper.get('publicationDate'),
            'publicationTypes': paper.get('publicationTypes', []),
            
            # Open Access - derived from status if needed
            'isOpenAccess': is_open_access,
            'openAccessPdf': self._parse_open_access(paper.get('openAccessPdf')),
            
            # Authors
            'authors': self._parse_authors(paper.get('authors', [])),
            
            # Citation metrics
            'citationCount': paper.get('citationCount', 0),
            'referenceCount': paper.get('referenceCount', 0),
            'influentialCitationCount': paper.get('influentialCitationCount', 0),
            
            # Fields of study
            'fieldsOfStudy': paper.get('fieldsOfStudy', []),
            
            # External IDs
            'externalIds': paper.get('externalIds', {}),
            
            # Journal
            'journal': self._parse_journal(paper.get('journal')),
            
            # TL;DR
            'tldr': paper.get('tldr', {}).get('text') if paper.get('tldr') else None,
            
            # Citations with contexts and intents
            'citations': self._parse_citations(paper.get('citations', []))
        }
    
    def _parse_open_access(self, oa_data: Optional[Dict]) -> Optional[Dict]:
        """Parse open access PDF information."""
        if not oa_data:
            return None
        
        return {
            'url': oa_data.get('url'),
            'status': oa_data.get('status'),
            'license': oa_data.get('license'),
            'disclaimer': oa_data.get('disclaimer')
        }
    
    def _parse_authors(self, authors: List[Dict]) -> List[Dict]:
        """Parse list of authors."""
        return [
            {
                'authorId': author.get('authorId'),
                'name': author.get('name'),
                'affiliations': author.get('affiliations', []),
                'url': author.get('url')
            }
            for author in authors
        ]
    
    def _parse_journal(self, journal: Optional[Dict]) -> Optional[Dict]:
        """Parse journal information."""
        if not journal:
            return None
        
        return {
            'name': journal.get('name'),
            'volume': journal.get('volume'),
            'pages': journal.get('pages')
        }
    
    def _parse_citations(self, citations: List[Dict]) -> List[Dict]:
        """Parse citation data including contexts and intents."""
        if not citations:
            return []
        
        parsed_citations = []
        for citation in citations:
            citing_paper = citation.get('citingPaper', {})
            
            parsed_citation = {
                'contexts': citation.get('contexts', []),
                'intents': citation.get('intents', []),
                'isInfluential': citation.get('isInfluential', False),
                'citingPaper': {
                    'paperId': citing_paper.get('paperId'),
                    'title': citing_paper.get('title'),
                    'year': citing_paper.get('year'),
                    'authors': self._parse_citation_authors(citing_paper.get('authors', []))
                } if citing_paper else None
            }
            parsed_citations.append(parsed_citation)
        
        return parsed_citations
    
    def _parse_citation_authors(self, authors: List[Dict]) -> List[Dict]:
        """Parse authors from citation data."""
        if not authors:
            return []
        
        return [
            {
                'authorId': author.get('authorId'),
                'name': author.get('name')
            }
            for author in authors
        ]
    
    def parse_papers(self, papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse a list of paper dictionaries.
        
        Args:
            papers: List of raw paper dictionaries from API
            
        Returns:
            List of cleaned and structured paper dictionaries
        """
        self.parsed_papers = [self.parse_paper(paper) for paper in papers]
        return self.parsed_papers
    
    def to_json(self, filename: str = 'parsed_papers.json', indent: int = 2, ensure_ascii: bool = False):
        """
        Save parsed papers to JSON file in outputs folder.
        
        Args:
            filename: Name of the output file
            indent: JSON indentation level
            ensure_ascii: Whether to escape non-ASCII characters
        """
        output_path = self.output_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_papers, f, indent=indent, ensure_ascii=ensure_ascii)
        
        print(f"✓ Saved {len(self.parsed_papers)} papers to {output_path}")
        return output_path
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert parsed papers to pandas DataFrame.
        
        Returns:
            DataFrame with flattened paper data
        """
        if not self.parsed_papers:
            return pd.DataFrame()
        
        # Flatten nested structures for DataFrame
        flattened = []
        for paper in self.parsed_papers:
            citations = paper.get('citations', [])
            
            # Calculate citation statistics
            total_contexts = sum(len(c.get('contexts', [])) for c in citations)
            total_intents = sum(len(c.get('intents', [])) for c in citations)
            
            flat = {
                'paperId': paper['paperId'],
                'title': paper['title'],
                'year': paper['year'],
                'abstract': paper['abstract'],
                'citationCount': paper['citationCount'],
                'referenceCount': paper['referenceCount'],
                'influentialCitationCount': paper['influentialCitationCount'],
                'venue': paper['venue'],
                'url': paper['url'],
                
                # Authors (comma-separated names)
                'authors': ', '.join([a['name'] for a in paper.get('authors', [])]),
                'author_count': len(paper.get('authors', [])),
                
                # External IDs
                'doi': paper.get('externalIds', {}).get('DOI'),
                'arxiv_id': paper.get('externalIds', {}).get('ArXiv'),
                'pubmed_id': paper.get('externalIds', {}).get('PubMed'),
                
                # Journal
                'journal_name': paper.get('journal', {}).get('name') if paper.get('journal') else None,
                
                # Open Access
                'open_access_url': paper.get('openAccessPdf', {}).get('url') if paper.get('openAccessPdf') else None,
                
                # TL;DR
                'tldr': paper.get('tldr'),
                
                # Fields of study
                'fields_of_study': ', '.join(paper.get('fieldsOfStudy', [])),
                
                # Citation data - detailed statistics
                'citations_fetched': len(citations),
                'influential_citations': sum(1 for c in citations if c.get('isInfluential')),
                'citations_with_context': sum(1 for c in citations if c.get('contexts')),
                'citations_with_intents': sum(1 for c in citations if c.get('intents')),
                'total_citation_contexts': total_contexts,
                'total_citation_intents': total_intents,
                'avg_contexts_per_citation': total_contexts / len(citations) if citations else 0,
                'avg_intents_per_citation': total_intents / len(citations) if citations else 0
            }
            flattened.append(flat)
        
        return pd.DataFrame(flattened)
    
    def to_csv(self, filename: str = 'parsed_papers.csv'):
        """
        Save parsed papers to CSV file in outputs folder.
        
        Args:
            filename: Name of the output file
        """
        output_path = self.output_dir / filename
        
        df = self.to_dataframe()
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"✓ Saved {len(df)} papers to {output_path}")
        return output_path
    
    def to_excel(self, filename: str = 'parsed_papers.xlsx'):
        """
        Save parsed papers to Excel file in outputs folder.
        
        Args:
            filename: Name of the output file
        """
        output_path = self.output_dir / filename
        
        df = self.to_dataframe()
        df.to_excel(output_path, index=False, engine='openpyxl')
        print(f"✓ Saved {len(df)} papers to {output_path}")
        return output_path
    
    def save_all_formats(self, base_filename: str = 'papers'):
        """
        Save parsed papers in all formats (JSON, CSV, Excel).
        
        Args:
            base_filename: Base name for output files (without extension)
            
        Returns:
            Dictionary with paths to all saved files
        """
        paths = {
            'json': self.to_json(f'{base_filename}.json'),
            'csv': self.to_csv(f'{base_filename}.csv'),
        }
        
        try:
            paths['excel'] = self.to_excel(f'{base_filename}.xlsx')
        except ImportError:
            print("⚠ Excel export requires openpyxl. Install with: pip install openpyxl")
        
        return paths
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the parsed papers.
        
        Returns:
            Dictionary with statistics
        """
        if not self.parsed_papers:
            return {}
        
        df = self.to_dataframe()
        
        stats = {
            'total_papers': len(self.parsed_papers),
            'year_range': {
                'min': int(df['year'].min()) if pd.notna(df['year'].min()) else None,
                'max': int(df['year'].max()) if pd.notna(df['year'].max()) else None
            },
            'citation_stats': {
                'mean': float(df['citationCount'].mean()),
                'median': float(df['citationCount'].median()),
                'max': int(df['citationCount'].max())
            },
            'papers_with_abstract': int(df['abstract'].notna().sum()),
            'papers_with_doi': int(df['doi'].notna().sum()),
            'papers_with_open_access': int(df['open_access_url'].notna().sum()),
            'unique_venues': int(df['venue'].nunique()),
            'top_venues': df['venue'].value_counts().head(5).to_dict()
        }
        
        # Add citation context statistics if available
        if 'citations_fetched' in df.columns and df['citations_fetched'].sum() > 0:
            stats['citation_context_stats'] = {
                'papers_with_citations': int(df['citations_fetched'].gt(0).sum()),
                'total_citations_fetched': int(df['citations_fetched'].sum()),
                'avg_citations_per_paper': float(df['citations_fetched'].mean()),
                'total_influential': int(df['influential_citations'].sum()) if 'influential_citations' in df.columns else 0,
                'total_with_context': int(df['citations_with_context'].sum()) if 'citations_with_context' in df.columns else 0,
                'total_with_intents': int(df['citations_with_intents'].sum()) if 'citations_with_intents' in df.columns else 0,
                'total_contexts': int(df['total_citation_contexts'].sum()) if 'total_citation_contexts' in df.columns else 0,
                'total_intents': int(df['total_citation_intents'].sum()) if 'total_citation_intents' in df.columns else 0,
                'avg_contexts_per_citation': float(df[df['citations_fetched'] > 0]['avg_contexts_per_citation'].mean()),
                'avg_intents_per_citation': float(df[df['citations_fetched'] > 0]['avg_intents_per_citation'].mean())
            }
        
        return stats
    
    def save_statistics(self, filename: str = 'paper_statistics.json'):
        """
        Save statistics to JSON file in outputs folder.
        
        Args:
            filename: Name of the output file
        """
        output_path = self.output_dir / filename
        
        stats = self.get_statistics()
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        print(f"✓ Saved statistics to {output_path}")
        return output_path
    
    def filter_papers(
        self,
        min_citations: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        has_abstract: bool = False,
        has_open_access: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Filter parsed papers based on criteria.
        
        Args:
            min_citations: Minimum citation count
            year_from: Minimum year
            year_to: Maximum year
            has_abstract: Only papers with abstracts
            has_open_access: Only open access papers
            
        Returns:
            Filtered list of papers
        """
        filtered = self.parsed_papers.copy()
        
        if min_citations is not None:
            filtered = [p for p in filtered if p['citationCount'] >= min_citations]
        
        if year_from is not None:
            filtered = [p for p in filtered if p['year'] and p['year'] >= year_from]
        
        if year_to is not None:
            filtered = [p for p in filtered if p['year'] and p['year'] <= year_to]
        
        if has_abstract:
            filtered = [p for p in filtered if p['abstract']]
        
        if has_open_access:
            filtered = [p for p in filtered if p['openAccessPdf'] and p['openAccessPdf'].get('url')]
        
        return filtered