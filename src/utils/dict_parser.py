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
            
            # Open Access
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
            
            # Journal info
            'journal': self._parse_journal(paper.get('journal')),
            
            # TL;DR
            'tldr': self._parse_tldr(paper.get('tldr'))
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
    
    def _parse_tldr(self, tldr: Optional[Dict]) -> Optional[str]:
        """Parse TL;DR summary."""
        if not tldr:
            return None
        return tldr.get('text')
    
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
                'fields_of_study': ', '.join(paper.get('fieldsOfStudy', []))
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
        
        return {
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


# Example usage
if __name__ == "__main__":
    import sys
    sys.path.append('../../')
    
    from src.Stage1_fetching.fetching import SemanticScholarClient
    
    # Fetch papers
    print("Fetching papers from Semantic Scholar...")
    client = SemanticScholarClient()
    papers = client.search_papers(query="Transformers", limit=20)
    
    # Parse papers
    print("\nParsing papers...")
    parser = PaperDictParser()  # Automatically uses 'outputs' folder
    parsed = parser.parse_papers(papers)
    
    # Save in all formats
    print("\nSaving results...")
    saved_files = parser.save_all_formats('transformer_papers')
    
    # Save statistics
    parser.save_statistics('transformer_papers_stats.json')
    
    # Print summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    stats = parser.get_statistics()
    print(json.dumps(stats, indent=2))
    
    # Filter highly cited recent papers
    highly_cited = parser.filter_papers(
        min_citations=50,
        year_from=2020,
        has_abstract=True
    )
    print(f"\n✓ Found {len(highly_cited)} highly cited recent papers")
    
    # Save filtered results
    if highly_cited:
        filtered_parser = PaperDictParser()
        filtered_parser.parsed_papers = highly_cited
        filtered_parser.save_all_formats('highly_cited_papers')